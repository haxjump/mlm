use std::sync::Arc;

use creep::Context;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use parking_lot::RwLock;

use crate::error::ConsensusError;
use crate::state::process::State;
use crate::types::{Address, MlmMsg, Node};
use crate::DurationConfig;
use crate::{smr::SMR, timer::Timer};
use crate::{Codec, Consensus, ConsensusResult, Crypto, Wal};

type Pile<T> = RwLock<Option<T>>;

/// An mlm consensus instance.
pub struct Mlm<T: Codec, F: Consensus<T>, C: Crypto, W: Wal> {
    sender: Pile<UnboundedSender<(Context, MlmMsg<T>)>>,
    state_rx: Pile<UnboundedReceiver<(Context, MlmMsg<T>)>>,
    address: Pile<Address>,
    consensus: Pile<Arc<F>>,
    crypto: Pile<Arc<C>>,
    wal: Pile<Arc<W>>,
}

impl<T, F, C, W> Mlm<T, F, C, W>
where
    T: Codec + Send + Sync + 'static,
    F: Consensus<T> + 'static,
    C: Crypto + Send + Sync + 'static,
    W: Wal + 'static,
{
    /// Create a new mlm and return an mlm instance with an unbounded receiver.
    pub fn new(
        address: Address,
        consensus: Arc<F>,
        crypto: Arc<C>,
        wal: Arc<W>,
    ) -> Self {
        let (tx, rx) = unbounded();
        Mlm {
            sender: RwLock::new(Some(tx)),
            state_rx: RwLock::new(Some(rx)),
            address: RwLock::new(Some(address)),
            consensus: RwLock::new(Some(consensus)),
            crypto: RwLock::new(Some(crypto)),
            wal: RwLock::new(Some(wal)),
        }
    }

    /// Get the mlm handler from the mlm instance.
    pub fn get_handler(&self) -> MlmHandler<T> {
        let sender = self.sender.write();
        assert!(sender.is_some());
        let tx = sender.clone().unwrap();
        MlmHandler::new(tx)
    }

    /// Run mlm consensus process. The `interval` is the height interval as millisecond.
    pub async fn run(
        &self,
        init_height: u64,
        interval: u64,
        authority_list: Vec<Node>,
        timer_config: Option<DurationConfig>,
    ) -> ConsensusResult<()> {
        let (mut smr_provider, evt_state, evt_timer) = SMR::new();
        let smr_handler = smr_provider.take_smr();
        let timer = Timer::new(evt_timer, smr_handler.clone(), interval, timer_config);
        let (verify_sig_tx, verify_sig_rx) = unbounded();

        let (rx, mut state, resp) = {
            let mut state_rx = self.state_rx.write();
            let mut address = self.address.write();
            let mut consensus = self.consensus.write();
            let mut crypto = self.crypto.write();
            let mut wal = self.wal.write();
            // let sender = self.sender.read();

            let tmp_rx = state_rx.take().unwrap();
            let (tmp_state, tmp_resp) = State::new(
                smr_handler,
                address.take().unwrap(),
                init_height,
                interval,
                authority_list,
                verify_sig_tx,
                consensus.take().unwrap(),
                crypto.take().unwrap(),
                wal.take().unwrap(),
            );

            // assert!(sender.is_none());
            assert!(address.is_none());
            assert!(consensus.is_none());
            assert!(crypto.is_none());
            assert!(state_rx.is_none());
            assert!(wal.is_none());

            (tmp_rx, tmp_state, tmp_resp)
        };

        log::info!("Mlm start running");

        // Run SMR.
        smr_provider.run();

        // Run timer.
        timer.run();

        // Run state.
        state.run(rx, evt_state, resp, verify_sig_rx).await;

        Ok(())
    }
}

/// An mlm handler to send messages to an mlm instance.
#[derive(Clone, Debug)]
pub struct MlmHandler<T: Codec>(UnboundedSender<(Context, MlmMsg<T>)>);

impl<T: Codec> MlmHandler<T> {
    fn new(tx: UnboundedSender<(Context, MlmMsg<T>)>) -> Self {
        MlmHandler(tx)
    }

    /// Send mlm message to the instance. Return `Err()` when the message channel is closed.
    pub fn send_msg(&self, ctx: Context, msg: MlmMsg<T>) -> ConsensusResult<()> {
        let ctx = match muta_apm::MUTA_TRACER.span(
            "mlm.send_msg_to_inner",
            vec![muta_apm::rustracing::tag::Tag::new("kind", "mlm")],
        ) {
            Some(mut span) => {
                span.log(|log| {
                    log.time(std::time::SystemTime::now());
                });
                ctx.with_value("parent_span_ctx", span.context().cloned())
            }
            None => ctx,
        };

        if self.0.is_closed() {
            Err(ConsensusError::ChannelErr(
                "[MlmHandler]: channel closed".to_string(),
            ))
        } else {
            self.0.unbounded_send((ctx, msg)).map_err(|e| {
                ConsensusError::Other(format!("Send message error {:?}", e))
            })
        }
    }
}

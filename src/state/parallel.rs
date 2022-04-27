use std::sync::Arc;

use bytes::Bytes;
use creep::Context;
use futures::channel::mpsc::UnboundedSender;
use muta_apm::derive::tracing_span;

use crate::types::{Address, AggregatedVote, MlmMsg};
use crate::utils::auth_manage::AuthorityManage;
use crate::{Codec, ConsensusResult, Crypto};

#[tracing_span(kind = "mlm.vreify_sig_pool")]
pub async fn parallel_verify<T: Codec + 'static, C: Crypto + Sync + 'static>(
    ctx: Context,
    msg: MlmMsg<T>,
    crypto: Arc<C>,
    authority: AuthorityManage,
    tx: UnboundedSender<(Context, MlmMsg<T>)>,
) {
    let msg_clone = msg.clone();
    tokio::spawn(async move {
        match msg {
            MlmMsg::SignedProposal(sp) => {
                let hash = crypto.hash(Bytes::from(rlp::encode(&sp.proposal)));
                if let Err(err) = crypto.verify_signature(
                    sp.signature.clone(),
                    hash,
                    sp.proposal.proposer.clone(),
                ) {
                    log::error!(
                        "Mlm: verify {:?} proposal signature failed {:?}",
                        sp,
                        err
                    );
                    return;
                }

                if let Some(polc) = sp.proposal.lock {
                    verify_qc(
                        ctx.clone(),
                        crypto,
                        polc.lock_votes,
                        authority,
                        tx.clone(),
                        msg_clone.clone(),
                    );
                } else {
                    let _ = tx.unbounded_send((ctx, msg_clone));
                }
            }

            MlmMsg::SignedVote(sv) => {
                let hash = crypto.hash(Bytes::from(rlp::encode(&sv.vote)));
                crypto
                    .verify_signature(sv.signature.clone(), hash, sv.voter.clone())
                    .map_or_else(
                        |err| {
                            log::error!(
                                "Mlm: verify {:?} vote signature failed {:?}",
                                sv,
                                err
                            );
                        },
                        |_| {
                            let _ = tx.unbounded_send((ctx, msg_clone));
                        },
                    );
            }

            MlmMsg::AggregatedVote(qc) => {
                verify_qc(ctx, crypto, qc, authority, tx, msg_clone);
            }

            MlmMsg::SignedChoke(sc) => {
                let hash = crypto.hash(Bytes::from(rlp::encode(&sc.choke.to_hash())));
                crypto
                    .verify_signature(sc.signature.clone(), hash, sc.address.clone())
                    .map_or_else(
                        |err| {
                            log::error!(
                                "Mlm: verify {:?} choke signature failed {:?}",
                                sc,
                                err
                            );
                        },
                        |_| {
                            let _ = tx.unbounded_send((ctx, msg_clone));
                        },
                    )
            }

            _ => (),
        }
    });
}

fn get_voters(
    addr_bitmap: &Bytes,
    authority_manage: AuthorityManage,
) -> ConsensusResult<Vec<Address>> {
    authority_manage.is_above_threshold(addr_bitmap)?;
    authority_manage.get_voters(addr_bitmap)
}

fn verify_qc<T: Codec, C: Crypto>(
    ctx: Context,
    crypto: Arc<C>,
    qc: AggregatedVote,
    authority: AuthorityManage,
    tx: UnboundedSender<(Context, MlmMsg<T>)>,
    msg_clone: MlmMsg<T>,
) {
    let hash = crypto.hash(Bytes::from(rlp::encode(&qc.to_vote())));
    if let Ok(voters) = get_voters(&qc.signature.address_bitmap, authority) {
        crypto
            .verify_aggregated_signature(qc.signature.signature.clone(), hash, voters)
            .map_or_else(
                |err| {
                    log::error!(
                        "Mlm: verify {:?} aggregated signature error {:?}",
                        qc,
                        err
                    );
                },
                |_| {
                    let _ = tx.unbounded_send((ctx, msg_clone));
                },
            );
    }
}

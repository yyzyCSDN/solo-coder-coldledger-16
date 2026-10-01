from datetime import timezone
from decimal import Decimal
def reference(device,excursion,model_key):
    start=excursion.start.astimezone(timezone.utc).isoformat()
    return f'exc:{device}:{start}:{model_key}'
def quantity(excursion):
    return Decimal(str((excursion.end-excursion.start).total_seconds()))
def post(ledger,account,device,excursions,model_key,reason='temperature-excursion'):
    """Post excursion claims idempotently under one correction model.

    The idempotency reference embeds the clock-correction model key, so:
    - replay with the same model finds every reference already posted and
      adds nothing (no double billing);
    - posting under a revised model first reverses this device's claims
      from superseded models, then posts the corrected claims, so the
      account balance reflects the current model's event times exactly
      once -- even when the revision shifted the excursion timestamps.
    """
    prefix=f'exc:{device}:'
    reversed_seqs={e.reversal_of for e in ledger.entries(account) if e.reversal_of is not None}
    for old in ledger.entries(account):
        if old.reversal_of is None and old.reference.startswith(prefix) and not old.reference.endswith(':'+model_key) and old.sequence not in reversed_seqs:
            ledger.reverse(old.sequence,old.reference+':rev','clock-correction revised')
    existing={e.reference for e in ledger.entries(account)}
    posted=[]
    for exc in excursions:
        ref=reference(device,exc,model_key); q=quantity(exc)
        if ref in existing or q==0: continue
        posted.append(ledger.post(account,ref,q,reason))
    return tuple(posted)

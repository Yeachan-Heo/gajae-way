# Bootstrap ceremony

Configure a valid profile first, then run the explicit one-time command while no
`way serve` process owns the state directory:

```text
way bootstrap --confirm --state-dir /var/lib/gajae-way --profile /etc/gajae-way/profile.toml
```

The daemon never creates a main session automatically. Bootstrap durably records
`CREATING {nonce, ts}` before SDK creation; the nonce is embedded in the first
bootstrap transcript message. A successful commit atomically binds the full
transcript fingerprint and profile digest.

After an interrupted ceremony, the next boot scans for the nonce: one matching
transcript is committed, zero matches restores `ABSENT` so an operator can run
this command again, and multiple matches fail closed for manual investigation.

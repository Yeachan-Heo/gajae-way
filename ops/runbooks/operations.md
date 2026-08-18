# Operations

## Discord adapter live verification

Use this procedure after configuring the static v1 owner-DM route. Keep the bot
credential in a systemd credential file or environment reference; never put its
value in the profile or command line.

Enable the bot's **Direct Messages** and **Message Content** gateway intents in
its Discord application before starting the adapter; the static v1 route reads
only the configured owner DM.

1. Configure the profile's mutable adapter section with the owner DM channel
   snowflake and the matching digest-bound owner surface ID:

   ```toml
   [adapter.discord]
   token_env = "WAY_DISCORD_BOT_TOKEN"
   channel_id = "123456789012345678"
   surface_id = "discord:owner-dm"
   ```

   `token_file = "/run/credentials/way-discord-token"` is the alternative to
   `token_env`. The configured surface must already be an owner `discord` `dm`
   surface in the profile.

2. Start exactly one healthy `way` gateway for the corpus, then verify its UDS
   endpoint and the Discord credential without opening a bot session:

   ```sh
   way-discord --check --state-dir /var/lib/gajae-way --profile /etc/gajae-way/profile.toml
   ```

   The command prints an `ok` result only after Discord `GET /users/@me` and
   gateway `way.health` both succeed.

3. Start one `way-discord` process with the same state directory and profile.
   Send a unique short message to the configured owner DM. Observe a typing
   acknowledgement within two seconds of delivery, then observe the assistant
   reply in that same DM. The typing acknowledgement must not wait for the
   persona response.

4. Restart `way-discord` after the reply has arrived and confirm the message is
   not posted again. For a delivery failure drill, stop it after an outbound
   send but before settlement, restart it, and inspect Discord for at most the
   bounded retry duplicate carrying the same nonce. The gateway checkpoint is
   authoritative: do not delete adapter-local files because the adapter has no
   durable local delivery state.

5. Investigate a failed check or repeated delivery before retrying with another
   adapter process. Do not run two gateways or two Discord adapters for the
   same configured route at once.

import Config

config :amqp,
  connections: [
    connection: [url: System.get_env("CLOUDAMQP_URL") || "amqp://guest:guest@localhost:5672/",
    ssl_options: [
        verify: :verify_none,
        fail_if_no_peer_cert: false
      ]

    ]
  ],
  channels: [
    channel: [connection: :connection]
  ]

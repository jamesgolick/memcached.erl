-record(packet, {
    op :: atom(),
    status :: atom(),
    key :: binary(),
    value = <<>> :: binary(),
    extra = <<>> :: binary()
  }).

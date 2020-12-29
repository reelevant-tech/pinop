<div align="center">

  <h1>pinop</h1>
  <h2> Apache <b>Pino</b>t reverse <b>p</b>roxy</h2>

  This tools is inspired from [Uber pinot rest proxy](https://eng.uber.com/operating-apache-pinot/), the purpose is to be able to query apache pinot's controller, and brokers for queries
</div>

## How to use

```bash
PINOT_CONTROLLER_URL=http://pinot-controller:9000 pinop
```

For available env variables see [main.go](./src/main.go:13)


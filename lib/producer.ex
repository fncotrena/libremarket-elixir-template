defmodule Producer do

  use AMQP

  defmodule Message do
    defstruct [:content, :vector_clock]
  end

  defmodule VectorClock do
    defstruct [:compras, :infracciones, :ventas, :envios, :pagos]

    def new do
      %VectorClock{
        compras: 0,
        infracciones: 0,
        ventas: 0,
        envios: 0,
        pagos: 0
      }
    end
  end

  def send_message(queue_name, message) do
    {:ok, channel} = get_channel()
    Queue.declare(channel, queue_name, durable: true)
    Basic.publish(channel, "", queue_name, message, persistent: true)
    IO.puts("Mensaje enviado a #{queue_name}: #{message}")
  end

  def get_channel(intentos \\ 30) do
    case AMQP.Application.get_channel(:channel) do
      {:ok, channel} ->
        {:ok, channel}

      {:error, _motivo} when intentos > 1 ->
        Process.sleep(1_000)
        get_channel(intentos - 1)

      error ->
        error
    end
  end

  def read_message(raw_message) do
    IO.puts("Mensaje recibido: #{raw_message}")
    raw_message
  end
end

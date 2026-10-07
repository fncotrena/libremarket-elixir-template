defmodule Libremarket.Supervisor do
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children =
      case System.get_env("SERVER_TO_RUN") do
        nil ->
          [
            {Libremarket.Compras.Server, %{}},
            {Libremarket.Ventas.Server, %{}},
            {Libremarket.Pagos.Server, %{}},
            {Libremarket.Infracciones.Server, %{}},
            {Libremarket.Envios.Server, %{}}
          ]

        server_to_run ->
          [{String.to_existing_atom("Elixir." <> server_to_run), %{}}]
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end

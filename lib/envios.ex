defmodule Libremarket.Envios do
  def calcular_costo() do
    GenServer.call(Libremarket.Envios.Server, :calcular_costo)
  end

  def agendar_envio(id_compra, empresa \\ :correo_argentino) do
    GenServer.call(Libremarket.Envios.Server, {:agendar_envio, id_compra, empresa})
  end
end

defmodule Libremarket.Envios.Server do
  use GenServer

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call(:calcular_costo, _from, state) do
    costo = Enum.random(500..2000)
    {:reply, costo, state}
  end

  @impl true
  def handle_call({:agendar_envio, id_compra, empresa}, _from, state) do
    envio = %{empresa: empresa, estado: :agendado}
    new_state = Map.put(state, id_compra, envio)
    {:reply, envio, new_state}
  end
end

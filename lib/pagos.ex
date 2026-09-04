defmodule Libremarket.Pagos do

  def pago() do
    case Enum.random(1..100) <= 70 do
      true -> :autorizado
      false -> :rechazado
    end
  end

end


defmodule Libremarket.Pagos.Server do
  use GenServer

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def pago(pid \\ __MODULE__, id_compra) do
    GenServer.call(pid, {:pago, id_compra})
  end

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call({:pago, id_compra}, _from, state) do
    resultado = Libremarket.Pagos.pago()
    new_state = Map.put(state, id_compra, resultado)

    {:reply, resultado, new_state}
  end
end

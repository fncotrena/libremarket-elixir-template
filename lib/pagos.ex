defmodule Libremarket.Pagos do

  def pago() do
    case Enum.random([:autorizado, :rechazado]) do
      :autorizado ->
        "Pago autorizado."

      :rechazado ->
        "Pago rechazado."
    end
  end

end


defmodule Libremarket.Pagos.Server do
  use GenServer

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def pago(pid \\ __MODULE__) do
    GenServer.call(pid, :pago)
  end

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call(:pago, _from, state) do
    resultado = Libremarket.Pagos.pago()

    {:reply, resultado, state}
  end
end

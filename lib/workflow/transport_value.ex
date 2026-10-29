defprotocol Runic.Workflow.TransportValue do
  @moduledoc false
  @fallback_to_any true

  @spec encode(term(), term()) :: :full | {:compact, term()}
  def encode(value, parent)

  @spec decode(term(), term()) :: {:ok, term()} | {:error, term()}
  def decode(value, parent)
end

defimpl Runic.Workflow.TransportValue, for: Any do
  def encode(_value, _parent), do: :full
  def decode(value, _parent), do: {:ok, value}
end

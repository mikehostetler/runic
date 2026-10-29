defmodule Runic.Workflow.AsyncTransport do
  @moduledoc false

  import Kernel, except: [byte_size: 1]

  alias Runic.Workflow.CausalContext
  alias Runic.Workflow.Events.FactProduced
  alias Runic.Workflow.{Fact, Runnable, RunnableCompleted, RunnableDispatched, TransportValue}

  defmodule Completion do
    @moduledoc false

    defstruct [
      :status,
      :result,
      :events,
      :hook_apply_fns,
      :execution_data,
      :error,
      :lifecycle_events
    ]
  end

  defmodule ProducedFact do
    @moduledoc false

    defstruct [:producer_label, :weight]
  end

  defmodule Stored do
    @moduledoc false

    defstruct [:table, :key, :logical_bytes]
  end

  @type mode :: :full | :compact | :binary | :ets

  @spec open(mode()) :: :ets.tid() | nil
  def open(:ets) do
    :ets.new(__MODULE__, [
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])
  end

  def open(_mode), do: nil

  @spec close(mode(), :ets.tid() | nil) :: :ok
  def close(:ets, table) when is_reference(table) do
    :ets.delete(table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  def close(_mode, _table), do: :ok

  @spec prepare(Runnable.t(), mode()) :: Runnable.t() | binary() | Stored.t()
  def prepare(%Runnable{} = runnable, mode), do: prepare(runnable, mode, nil)

  @spec prepare(Runnable.t(), mode(), :ets.tid() | nil) :: Runnable.t() | binary() | Stored.t()
  def prepare(%Runnable{} = runnable, :full, _store), do: runnable
  def prepare(%Runnable{} = runnable, :compact, _store), do: compact_request(runnable)

  def prepare(%Runnable{} = runnable, :binary, _store) do
    runnable
    |> compact_request()
    |> :erlang.term_to_binary()
  end

  def prepare(%Runnable{} = runnable, :ets, table) when is_reference(table) do
    request = compact_request(runnable)
    store(table, request)
  end

  @spec execute(Runnable.t() | binary() | Stored.t(), (Runnable.t() -> term()), mode()) :: term()
  def execute(payload, execute, mode), do: execute(payload, execute, mode, nil)

  @spec execute(
          Runnable.t() | binary() | Stored.t(),
          (Runnable.t() -> term()),
          mode(),
          :ets.tid() | nil
        ) :: term()
  def execute(%Runnable{} = runnable, execute, :full, _store), do: execute.(runnable)

  def execute(%Runnable{} = runnable, execute, :compact, _store) do
    runnable
    |> restore_request()
    |> execute.()
    |> compact_completion()
  end

  def execute(binary, execute, :binary, _store) when is_binary(binary) do
    binary
    |> :erlang.binary_to_term()
    |> restore_request()
    |> execute.()
    |> compact_completion()
    |> :erlang.term_to_binary()
  end

  def execute(%Stored{table: table} = stored, execute, :ets, table) do
    stored
    |> take!()
    |> restore_request()
    |> execute.()
    |> compact_completion()
    |> then(&store(table, &1))
  end

  @spec accept(Runnable.t(), term(), mode()) :: term()
  def accept(original, result, mode), do: accept(original, result, mode, nil)

  @spec accept(Runnable.t(), term(), mode(), :ets.tid() | nil) :: term()
  def accept(_original, result, :full, _store), do: result

  def accept(%Runnable{} = original, %Completion{} = completion, :compact, _store) do
    restore_completion(original, completion)
  end

  def accept(%Runnable{} = original, binary, :binary, _store) when is_binary(binary) do
    completion = :erlang.binary_to_term(binary)
    restore_completion(original, completion)
  end

  def accept(%Runnable{} = original, %Stored{table: table} = stored, :ets, table) do
    completion = take!(stored)
    restore_completion(original, completion)
  end

  @spec byte_size(Runnable.t() | binary() | Stored.t()) :: non_neg_integer()
  def byte_size(binary) when is_binary(binary), do: Kernel.byte_size(binary)
  def byte_size(%Stored{logical_bytes: bytes}), do: bytes

  def byte_size(term) do
    :erts_debug.flat_size(term) * :erlang.system_info(:wordsize)
  end

  @spec estimate_bytes(Runnable.t(), mode()) :: non_neg_integer()
  def estimate_bytes(%Runnable{} = runnable, :full), do: byte_size(runnable)

  def estimate_bytes(%Runnable{} = runnable, :compact),
    do: runnable |> compact_request() |> byte_size()

  def estimate_bytes(%Runnable{} = runnable, :ets),
    do: runnable |> compact_request() |> byte_size()

  def estimate_bytes(%Runnable{} = runnable, :binary) do
    runnable
    |> compact_request()
    |> :erlang.term_to_binary()
    |> byte_size()
  end

  defp store(table, value) do
    key = make_ref()
    true = :ets.insert(table, {key, value})
    %Stored{table: table, key: key, logical_bytes: byte_size(value)}
  end

  defp take!(%Stored{table: table, key: key}) do
    case :ets.take(table, key) do
      [{^key, value}] -> value
      [] -> raise ArgumentError, "async transport value is not available"
    end
  end

  defp compact_request(%Runnable{context: %CausalContext{} = context} = runnable) do
    %{runnable | context: %{context | input_fact: nil}}
  end

  defp compact_request(%Runnable{} = runnable), do: runnable

  defp restore_request(
         %Runnable{input_fact: %Fact{} = fact, context: %CausalContext{input_fact: nil} = context} =
           runnable
       ) do
    %{runnable | context: %{context | input_fact: fact}}
  end

  defp restore_request(%Runnable{} = runnable), do: runnable

  defp compact_completion({%Runnable{} = runnable, lifecycle_events})
       when is_list(lifecycle_events) do
    build_completion(runnable, lifecycle_events)
  end

  defp compact_completion(%Runnable{} = runnable), do: build_completion(runnable, nil)

  defp build_completion(%Runnable{} = runnable, lifecycle_events) do
    result = compact_result(runnable.result, runnable.input_fact)

    %Completion{
      status: runnable.status,
      result: result,
      events: compact_events(runnable.events, runnable),
      hook_apply_fns: runnable.hook_apply_fns,
      execution_data: runnable.execution_data,
      error: runnable.error,
      lifecycle_events: compact_events(lifecycle_events, runnable)
    }
  end

  defp restore_completion(%Runnable{} = original, %Completion{} = completion) do
    result = restore_result(completion.result, original.input_fact)

    executed = %{
      original
      | status: completion.status,
        result: result,
        events: restore_events(completion.events, result, original.input_fact),
        hook_apply_fns: completion.hook_apply_fns,
        execution_data: completion.execution_data,
        error: completion.error
    }

    case completion.lifecycle_events do
      nil ->
        executed

      events ->
        {executed, restore_events(events, result, original.input_fact)}
    end
  end

  defp compact_result(%Fact{value: value} = result, %Fact{value: parent}) do
    case TransportValue.encode(value, parent) do
      :full -> result
      {:compact, encoded} -> %{result | value: encoded}
    end
  end

  defp compact_result(result, _input_fact), do: result

  defp restore_result(%Fact{value: value} = result, %Fact{value: parent}) do
    case TransportValue.decode(value, parent) do
      {:ok, decoded} -> %{result | value: decoded}
      {:error, reason} -> raise ArgumentError, "cannot decode async result: #{inspect(reason)}"
    end
  end

  defp restore_result(result, _input_fact), do: result

  defp compact_events(nil, _runnable), do: nil

  defp compact_events(events, %Runnable{} = runnable) when is_list(events) do
    Enum.map(events, &compact_event(&1, runnable))
  end

  defp compact_event(
         %FactProduced{hash: hash} = event,
         %Runnable{result: %Fact{hash: hash}}
       ) do
    %ProducedFact{producer_label: event.producer_label, weight: event.weight}
  end

  defp compact_event(
         %RunnableCompleted{result_fact: %Fact{hash: hash}} = event,
         %Runnable{result: %Fact{hash: hash}}
       ) do
    %{event | result_fact: nil}
  end

  defp compact_event(
         %RunnableDispatched{input_fact: %Fact{hash: hash}} = event,
         %Runnable{input_fact: %Fact{hash: hash}}
       ) do
    %{event | input_fact: nil}
  end

  defp compact_event(event, _runnable), do: event

  defp restore_events(nil, _result, _input_fact), do: nil

  defp restore_events(events, result, input_fact) when is_list(events) do
    Enum.map(events, &restore_event(&1, result, input_fact))
  end

  defp restore_event(
         %ProducedFact{producer_label: producer_label, weight: weight},
         %Fact{} = result,
         _input_fact
       ) do
    FactProduced.new(result, producer_label: producer_label, weight: weight)
  end

  defp restore_event(%RunnableCompleted{result_fact: nil} = event, %Fact{} = result, _input) do
    %{event | result_fact: result}
  end

  defp restore_event(%RunnableDispatched{input_fact: nil} = event, _result, %Fact{} = input) do
    %{event | input_fact: input}
  end

  defp restore_event(event, _result, _input_fact), do: event
end

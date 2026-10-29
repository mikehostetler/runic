defmodule Runic.AsyncTransportTest do
  use ExUnit.Case, async: true

  require Runic

  alias Runic.Workflow
  alias Runic.Workflow.{Execution, Fact, RunnableCompleted, RunnableDispatched}

  for mode <- [:compact, :binary, :ets] do
    test "#{mode} transport preserves results and lifecycle facts" do
      input = %{items: Enum.to_list(1..100), label: "transport"}

      workflow =
        Workflow.new()
        |> Workflow.add(
          Runic.step(fn value -> Map.put(value, :count, length(value.items)) end,
            name: :measure
          )
        )
        |> Workflow.enable_event_emission()

      {completed, execution} =
        Workflow.execute(workflow, input,
          async: true,
          max_concurrency: 2,
          async_transport: unquote(mode)
        )

      expected = Map.put(input, :count, 100)
      assert Execution.outputs(execution) == [expected]

      assert [%RunnableDispatched{input_fact: %Fact{value: ^input}}] =
               Enum.filter(completed.runnable_events, &is_struct(&1, RunnableDispatched))

      assert [%RunnableCompleted{result_fact: %Fact{value: ^expected}}] =
               Enum.filter(completed.runnable_events, &is_struct(&1, RunnableCompleted))
    end
  end

  test "the byte budget keeps concurrency and admits one large request at a time" do
    owner = self()

    blocking_step = fn name ->
      Runic.step(
        fn value ->
          send(owner, {:started, name, self()})

          receive do
            :finish -> {name, value}
          end
        end,
        name: name
      )
    end

    workflow = Runic.workflow(steps: [blocking_step.(:first), blocking_step.(:second)])

    task =
      Task.async(fn ->
        Workflow.execute(workflow, :input,
          async: true,
          max_concurrency: 2,
          max_inflight_bytes: 1,
          async_transport: :compact
        )
      end)

    assert_receive {:started, _name, first}, 1_000
    refute_receive {:started, _name, _pid}, 100
    send(first, :finish)

    assert_receive {:started, _name, second}, 1_000
    send(second, :finish)

    {_completed, execution} = Task.await(task)
    assert Enum.sort(Execution.outputs(execution)) == [{:first, :input}, {:second, :input}]
  end

  test "invalid async memory options fail before dispatch" do
    workflow = Runic.workflow(steps: [Runic.step(& &1, name: :identity)])

    assert_raise ArgumentError, ~r/max_inflight_bytes/, fn ->
      Workflow.execute(workflow, :input,
        async: true,
        max_inflight_bytes: 0
      )
    end

    assert_raise ArgumentError, ~r/async_transport/, fn ->
      Workflow.execute(workflow, :input,
        async: true,
        async_transport: :unknown
      )
    end

    assert_raise ArgumentError, ~r/async_placement/, fn ->
      Workflow.execute(workflow, :input,
        async: true,
        async_placement: :inline
      )
    end
  end

  test "placement runs orchestration inline and keeps selected work in tasks" do
    owner = self()

    inline =
      Runic.step(
        fn value ->
          send(owner, {:ran, :inline, self()})
          value
        end,
        name: :inline
      )

    task =
      Runic.step(
        fn value ->
          send(owner, {:ran, :task, self()})
          value
        end,
        name: :task
      )

    placement = fn runnable -> if runnable.node.name == :inline, do: :inline, else: :task end

    Workflow.execute(Runic.workflow(steps: [inline, task]), :input,
      async: true,
      max_concurrency: 2,
      async_placement: placement,
      async_transport: :compact
    )

    assert_receive {:ran, :inline, ^owner}
    assert_receive {:ran, :task, task_pid}
    refute task_pid == owner
  end
end

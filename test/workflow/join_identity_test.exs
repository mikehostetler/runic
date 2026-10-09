defmodule Runic.Workflow.JoinIdentityTest do
  use ExUnit.Case, async: true
  require Runic
  alias Runic.Workflow
  alias Runic.Workflow.{Fact, Invokable, Join}

  for mode <- [:invoke, :dispatch] do
    test "#{mode} Join identity is independent of parent completion order" do
      {workflow, join, left, right} = fixture(false)
      forward = complete(workflow, join, [left, right], unquote(mode))
      reverse = complete(workflow, join, [right, left], unquote(mode))
      assert forward.value == [:left, :right]
      assert reverse.value == forward.value
      assert reverse.hash == forward.hash
    end

    test "#{mode} Join keeps the deepest parent in either completion order" do
      {workflow, join, left, right} = fixture(true)
      forward = complete(workflow, join, [left, right], unquote(mode))
      reverse = complete(workflow, join, [right, left], unquote(mode))
      assert Workflow.ancestry_depth(workflow, forward) == 3
      assert Workflow.ancestry_depth(workflow, reverse) == 3
      assert reverse.hash == forward.hash
    end
  end

  for mode <- [:invoke, :dispatch] do
    test "#{mode} FanIn identity is independent of result completion order" do
      workflow =
        Runic.workflow(
          steps: [
            {Runic.map(fn value -> value * 2 end, name: :items),
             [
               Runic.reduce([], fn value, acc -> acc ++ [value] end,
                 name: :collected,
                 map: :items
               )
             ]}
          ]
        )
        |> Workflow.plan_eagerly([1, 2, 3])

      {prepared, arrivals} = until_fan_in(workflow, 10)
      forward = complete_fan_in(prepared, arrivals, unquote(mode))
      reverse = complete_fan_in(prepared, Enum.reverse(arrivals), unquote(mode))
      assert forward.value == [2, 4, 6]
      assert reverse.value == forward.value
      assert reverse.hash == forward.hash
    end
  end

  for mode <- [:invoke, :dispatch] do
    test "#{mode} empty Join retains the triggering parent" do
      {workflow, _join, left, _right} = fixture(false)
      empty = Join.new([])
      workflow = Workflow.draw_connection(workflow, left, empty, :runnable)
      completed = complete(workflow, empty, [left], unquote(mode))
      assert completed.value == []
      assert completed.ancestry == {empty.hash, left.hash}
    end
  end

  defp until_fan_in(workflow, remaining) when remaining > 0 do
    {prepared, runnables} = Workflow.prepare_for_dispatch(workflow)
    assert runnables != []

    if Enum.all?(runnables, &is_struct(&1.node, Runic.Workflow.FanIn)) do
      {prepared, runnables}
    else
      next =
        Enum.reduce(runnables, prepared, fn ready, wf ->
          Workflow.apply_runnable(wf, Invokable.execute(ready.node, ready))
        end)

      until_fan_in(next, remaining - 1)
    end
  end

  defp complete_fan_in(workflow, arrivals, mode) do
    completed =
      Enum.reduce(arrivals, workflow, fn ready, wf ->
        case mode do
          :invoke -> Invokable.invoke(ready.node, wf, ready.input_fact)
          :dispatch -> Workflow.apply_runnable(wf, Invokable.execute(ready.node, ready))
        end
      end)

    hash = hd(arrivals).node.hash

    Enum.find(Workflow.facts(completed), fn fact ->
      match?({producer, _} when producer == hash, fact.ancestry)
    end)
  end

  defp fixture(deep?) do
    left_node = Runic.step(fn value -> value end, name: :left)
    right_node = Runic.step(fn value -> value end, name: :right)
    bridge = Runic.step(fn value -> value end, name: :bridge)
    join = Join.new([left_node.hash, right_node.hash])

    workflow =
      Workflow.new()
      |> Workflow.add_steps([left_node, right_node, bridge])
      |> Workflow.add_step(left_node, join)
      |> Workflow.add_step(right_node, join)

    root = Fact.new(value: :root)
    middle = Fact.new(value: :middle, ancestry: {bridge.hash, root.hash})
    left = Fact.new(value: :left, ancestry: {left_node.hash, root.hash})

    right =
      Fact.new(
        value: :right,
        ancestry: {right_node.hash, if(deep?, do: middle.hash, else: root.hash)}
      )

    workflow =
      Enum.reduce([root, middle, left, right], workflow, &Workflow.log_fact(&2, &1))
      |> Workflow.draw_connection(left, join, :runnable)
      |> Workflow.draw_connection(right, join, :runnable)

    {workflow, join, left, right}
  end

  defp complete(workflow, join, facts, mode) do
    completed =
      Enum.reduce(facts, workflow, fn fact, wf ->
        case mode do
          :invoke ->
            Invokable.invoke(join, wf, fact)

          :dispatch ->
            {:ok, ready} = Invokable.prepare(join, wf, fact)
            Workflow.apply_runnable(wf, Invokable.execute(join, ready))
        end
      end)

    Enum.find(Workflow.facts(completed), fn fact ->
      match?({hash, _} when hash == join.hash, fact.ancestry)
    end)
  end
end

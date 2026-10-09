defmodule Runic.Workflow.SingleOutputLegacyHooksTest do
  use ExUnit.Case, async: true

  require Runic
  alias Runic.TestSupport.OrdinaryComponent
  alias Runic.Workflow
  alias Runic.Workflow.Invokable

  for kind <- [:step, :custom], path <- [:invoke, :execute] do
    test "#{kind} #{path} preserves input and output Facts in legacy hooks" do
      owner = self()

      node =
        case unquote(kind) do
          :step -> Runic.step(fn value -> value + 1 end, name: :increment)
          :custom -> OrdinaryComponent.new(:increment, :add, 1)
        end

      workflow =
        Workflow.new()
        |> Workflow.add(node)
        |> Workflow.plan_eagerly(1)
        |> Workflow.attach_before_hook(:increment, fn _, workflow, fact ->
          send(owner, {:before, fact})
          workflow
        end)
        |> Workflow.attach_after_hook(:increment, fn _, workflow, fact ->
          send(owner, {:after, fact})
          workflow
        end)

      [input] = Workflow.facts(workflow)

      completed =
        case unquote(path) do
          :invoke ->
            Invokable.invoke(node, workflow, input)

          :execute ->
            {:ok, runnable} = Invokable.prepare(node, workflow, input)
            Workflow.apply_runnable(workflow, Invokable.execute(node, runnable))
        end

      [output] = Workflow.productions(completed, :increment)
      assert output.value == 2
      assert_received {:before, ^input}
      assert_received {:after, ^output}
      refute_received {:before, _}
      refute_received {:after, _}
    end
  end
end

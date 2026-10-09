defmodule Runic.Runner.EncodingFailureTest do
  use ExUnit.Case, async: false
  @moduletag capture_log: true
  require Runic
  alias Runic.{Runner, Workflow}

  test "a sequential Promise does not repeat a rejected apply hook" do
    owner = self()

    workflow =
      Runic.workflow(
        steps: [
          {Runic.step(fn value -> value + 1 end, name: :first),
           [Runic.step(fn value -> value * 2 end, name: :second)]}
        ]
      )
      |> Workflow.attach_after_hook(:first, fn _, wf, _ ->
        send(owner, :hook_applied)
        Workflow.Fact.new(value: self())
        wf
      end)

    runner = :"encoding_failure_runner_#{System.unique_integer([:positive])}"
    start_supervised!({Runner, name: runner})

    {:ok, worker} =
      Runner.start_workflow(runner, :chain, workflow,
        scheduler: Runic.Runner.Scheduler.ChainBatching,
        on_complete: fn _, wf -> send(owner, {:done, wf}) end
      )

    assert :ok = Runner.run(runner, :chain, 1)
    assert_receive {:done, completed}, 2000
    assert_receive :hook_applied, 2000
    refute_received :hook_applied
    assert Process.alive?(worker)
    assert Workflow.raw_productions(completed, :second) == []
    assert {:ok, %{status: :stopped, active_units: 0}} = Runner.admission_status(runner, :chain)
    failures = Enum.filter(completed.runnable_events, &is_struct(&1, Workflow.RunnableFailed))
    assert [%{error: {:value_encoding_failed, %Runic.Identity.CanonicalError{}}}] = failures
  end
end

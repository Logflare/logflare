defmodule Logflare.AlertIssues.SilencesTest do
  use Logflare.DataCase, async: true

  alias Logflare.AlertIssues.Silence
  alias Logflare.AlertIssues.Silences

  setup do
    %{user: insert(:user)}
  end

  test "creates, lists and deletes silences per user", %{user: user} do
    assert {:ok, %Silence{id: id}} =
             Silences.create(user, %{
               "rule_key" => "logflare.edge-fn-5xx",
               "matchers" => %{"project" => "p1"},
               "created_by" => "ops"
             })

    other = insert(:user)
    assert [%Silence{id: ^id}] = Silences.list(user)
    assert [] == Silences.list(other)
    assert {:error, :not_found} = Silences.delete(other, id)
    assert :ok = Silences.delete(user, id)
    assert [] == Silences.list(user)
  end

  test "rejects invalid matchers", %{user: user} do
    assert {:error, changeset} =
             Silences.create(user, %{"matchers" => %{"Bad Key" => "x"}})

    assert %{matchers: [_]} = errors_on(changeset)
  end

  test "a silence matches its rule key and every matcher" do
    silence = %Silence{rule_key: "r1", matchers: %{"project" => "p1"}}

    assert Silence.matches?(silence, "r1", %{"project" => "p1", "function_id" => "f"})
    refute Silence.matches?(silence, "r2", %{"project" => "p1"})
    refute Silence.matches?(silence, "r1", %{"project" => "p2"})
    assert Silence.matches?(%{silence | rule_key: nil}, "r2", %{"project" => "p1"})
  end

  test "list_matching returns silences satisfied by the identifiers", %{user: user} do
    {:ok, s1} = Silences.create(user, %{"rule_key" => "r1", "matchers" => %{"project" => "p1"}})
    {:ok, _s2} = Silences.create(user, %{"rule_key" => "r1", "matchers" => %{"project" => "p2"}})

    assert [^s1] = Silences.list_matching(user, %{"project" => "p1"})
    assert ^s1 = Silences.find_exact([s1], "r1", %{"project" => "p1"})
    assert nil == Silences.find_exact([s1], "r1", %{"project" => "p2"})
  end
end

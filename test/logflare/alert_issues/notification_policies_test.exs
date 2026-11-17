defmodule Logflare.AlertIssues.NotificationPoliciesTest do
  use Logflare.DataCase, async: true

  alias Logflare.AlertIssues.NotificationPolicies
  alias Logflare.AlertIssues.NotificationPolicy

  @issue %{
    "rule_key" => "supabase.advisor.rls_disabled_in_public",
    "severity" => "major",
    "category" => "security",
    "identifiers" => %{"project" => "default", "schema" => "public"}
  }

  setup do
    %{user: insert(:user), other: insert(:user)}
  end

  defp policy(attrs \\ %{}) do
    struct(
      %NotificationPolicy{
        name: "ops",
        enabled: true,
        rule_keys: [],
        min_severity: nil,
        categories: [],
        identifiers: %{},
        events: ["opened", "reopened", "repeat"],
        receivers: ["ops-webhook"]
      },
      attrs
    )
  end

  describe "matches?/3" do
    test "an empty policy matches every alert issue for its events" do
      assert NotificationPolicy.matches?(policy(), "opened", @issue)
      refute NotificationPolicy.matches?(policy(), "resolved", @issue)
    end

    test "rule key globs" do
      assert NotificationPolicy.matches?(
               policy(rule_keys: ["supabase.advisor.*"]),
               "opened",
               @issue
             )

      assert NotificationPolicy.matches?(
               policy(rule_keys: ["custom.*", "*rls*"]),
               "opened",
               @issue
             )

      refute NotificationPolicy.matches?(policy(rule_keys: ["supabase.edge-*"]), "opened", @issue)
      refute NotificationPolicy.matches?(policy(rule_keys: ["supabase"]), "opened", @issue)

      refute NotificationPolicy.matches?(
               policy(rule_keys: ["supabase.advisor.*"]),
               "opened",
               Map.put(@issue, "rule_key", nil)
             )
    end

    test "glob special characters other than * are literal" do
      refute NotificationPolicy.matches?(
               policy(rule_keys: ["supabase.advisor.rls_disabled_in_public."]),
               "opened",
               @issue
             )

      refute NotificationPolicy.matches?(
               policy(rule_keys: ["supabaseXadvisor.*"]),
               "opened",
               @issue
             )
    end

    test "minimum severity is a threshold" do
      assert NotificationPolicy.matches?(policy(min_severity: :warning), "opened", @issue)
      assert NotificationPolicy.matches?(policy(min_severity: :major), "opened", @issue)
      refute NotificationPolicy.matches?(policy(min_severity: :critical), "opened", @issue)
    end

    test "categories and identifiers" do
      assert NotificationPolicy.matches?(policy(categories: ["security"]), "opened", @issue)
      refute NotificationPolicy.matches?(policy(categories: ["health"]), "opened", @issue)

      assert NotificationPolicy.matches?(
               policy(identifiers: %{"project" => "default"}),
               "opened",
               @issue
             )

      refute NotificationPolicy.matches?(
               policy(identifiers: %{"project" => "other"}),
               "opened",
               @issue
             )
    end

    test "disabled policies match nothing" do
      refute NotificationPolicy.matches?(policy(enabled: false), "opened", @issue)
    end
  end

  describe "create/2" do
    test "creates a policy with defaults", %{user: user} do
      assert {:ok, policy} =
               NotificationPolicies.create(user, %{
                 "name" => "advisors",
                 "rule_keys" => ["supabase.advisor.*"],
                 "min_severity" => "warning",
                 "receivers" => ["ops-webhook"]
               })

      assert policy.enabled
      assert policy.events == ["opened", "reopened", "repeat"]
      assert policy.min_severity == :warning
      assert policy.repeat_interval_seconds == nil
    end

    test "validates fields", %{user: user} do
      assert {:error, changeset} =
               NotificationPolicies.create(user, %{
                 "name" => "",
                 "rule_keys" => ["Bad Key"],
                 "categories" => ["billing"],
                 "identifiers" => %{"Project" => "x"},
                 "events" => ["paged"],
                 "repeat_interval_seconds" => 60,
                 "receivers" => []
               })

      errors = errors_on(changeset)

      for field <- [
            :name,
            :rule_keys,
            :categories,
            :identifiers,
            :events,
            :repeat_interval_seconds,
            :receivers
          ] do
        assert Map.has_key?(errors, field), "expected an error on #{field}"
      end
    end

    test "names are unique per user", %{user: user, other: other} do
      attrs = %{"name" => "ops", "receivers" => ["ops-webhook"]}
      assert {:ok, _} = NotificationPolicies.create(user, attrs)
      assert {:error, changeset} = NotificationPolicies.create(user, attrs)
      assert %{name: [_]} = errors_on(changeset)
      assert {:ok, _} = NotificationPolicies.create(other, attrs)
    end
  end

  describe "scoping" do
    setup %{user: user} do
      {:ok, policy} =
        NotificationPolicies.create(user, %{"name" => "ops", "receivers" => ["ops-webhook"]})

      %{policy: policy}
    end

    test "get, update and delete are scoped to the user", %{
      user: user,
      other: other,
      policy: policy
    } do
      assert {:ok, %{id: id}} = NotificationPolicies.get(user, policy.id)
      assert id == policy.id
      assert {:error, :not_found} = NotificationPolicies.get(other, policy.id)
      assert {:error, :not_found} = NotificationPolicies.get(user, "nope")

      assert {:ok, updated} =
               NotificationPolicies.update(user, policy.id, %{"enabled" => false})

      refute updated.enabled
      assert {:error, :not_found} = NotificationPolicies.update(other, policy.id, %{})
      assert {:error, :not_found} = NotificationPolicies.delete(other, policy.id)
      assert :ok = NotificationPolicies.delete(user, policy.id)
      assert NotificationPolicies.list(user) == []
    end

    test "list_enabled only returns enabled policies for a user id", %{user: user} do
      {:ok, _} =
        NotificationPolicies.create(user, %{
          "name" => "off",
          "enabled" => false,
          "receivers" => ["x"]
        })

      assert [%{name: "ops"}] = NotificationPolicies.list_enabled(user.id)
    end
  end
end

defmodule LogflareWeb.RuleController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  plug LogflareWeb.Plugs.SetVerifySource when action in [:index, :delete, :create]

  alias Logflare.{Rule, Source}

  def create(conn, %{"rule" => rule}) do
    %{assigns: %{user: user, source: source}} = conn
    disabled_source = source.token

    changeset =
      source
      |> Ecto.build_assoc(:rules)
      |> Rule.changeset(rule)

    sources =
      for s <- user.sources do
        if disabled_source == source.token,
          do: Map.put(s, :disabled, true),
          else: Map.put(s, :disabled, false)
      end

    case Repo.insert(changeset) do
      {:ok, _rule} ->
        conn
        |> put_flash(:info, "Rule created successfully!")
        |> redirect(to: Routes.source_rule_path(conn, :index, source.id))

      {:error, %Ecto.Changeset{} = changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("index.html",
          rules: source.rules,
          source: source,
          changeset: changeset,
          sources: sources
        )
    end
  end

  def index(conn, _) do
    %{assigns: %{user: user, source: current_source}} = conn

    changeset = Rule.changeset(%Rule{source: current_source.id})

    source =
      current_source
      |> Repo.preload([:rules], force: true)

    sources =
      for s <- user.sources do
        if s.token == current_source.token do
          Map.put(s, :disabled, true)
        else
          Map.put(s, :disabled, false)
        end
      end

    render(conn, "index.html",
      rules: source.rules,
      source: source,
      changeset: changeset,
      sources: sources
    )
  end

  def delete(conn, %{"id" => rule_id, "source_id" => source_id}) do
    Repo.get!(Rule, rule_id) |> Repo.delete!()

    conn
    |> put_flash(:info, "Rule deleted!")
    |> redirect(to: Routes.source_rule_path(conn, :index, source_id))
  end
end

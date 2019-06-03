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

  def index(conn, %{"source_id" => source_id}) do
    user_id = conn.assigns.user.id
    source = Repo.get(Source, source_id)
    disabled_source = source.token
    source_id_int = String.to_integer(source_id)

    changeset = Rule.changeset(%Rule{source: source_id})

    rules_query =
      from(r in "rules",
        where: r.source_id == ^source_id_int,
        select: %{
          id: r.id,
          regex: r.regex,
          sink: r.sink
        }
      )

    sources_query =
      from(s in "sources",
        where: s.user_id == ^user_id,
        order_by: s.name,
        select: %{
          name: s.name,
          id: s.id,
          token: s.token
        }
      )

    rules =
      for rule <- Repo.all(rules_query) do
        {:ok, sink} = Ecto.UUID.load(rule.sink)
        Map.put(rule, :sink, sink)
      end

    sources =
      for source <- Repo.all(sources_query) do
        {:ok, token} = Ecto.UUID.Atom.load(source.token)
        s = Map.put(source, :token, token)

        case token do
          ^disabled_source ->
            Map.put(s, :disabled, true)

          _source ->
            Map.put(s, :disabled, false)
        end
      end

    render(conn, "index.html",
      rules: rules,
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

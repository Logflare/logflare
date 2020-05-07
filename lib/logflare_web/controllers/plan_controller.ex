defmodule LogflareWeb.AdminPlanController do
  use LogflareWeb, :controller

  alias Logflare.Plans

  def index(conn, _params) do
    plans = Plans.list_plans()

    conn
    |> render("index.html", plans: plans)
  end

  def new(conn, _params) do
    changeset = Plans.Plan.changeset(%Plans.Plan{}, %{})

    conn
    |> render("new.html", changeset: changeset)
  end

  def create(conn, %{"plan" => params}) do
    IO.inspect(params)

    case Plans.create_plan(params) do
      {:ok, _plan} ->
        conn
        |> put_flash(:info, "Plan created!")
        |> redirect(to: Routes.admin_plan_path(conn, :index))

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("new.html", changeset: changeset)
    end
  end
end

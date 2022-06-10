defmodule LogflareWeb.AdminPlanController do
  use LogflareWeb, :controller

  alias Logflare.Billing

  def index(conn, _params) do
    plans = Billing.list_plans() |> Enum.sort_by(& &1.id, :asc)

    conn
    |> render("index.html", plans: plans)
  end

  def new(conn, _params) do
    changeset = Billing.Plan.changeset(%Billing.Plan{}, %{})

    conn
    |> render("new.html", changeset: changeset)
  end

  def create(conn, %{"plan" => params}) do
    case Billing.create_plan(params) do
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

  def edit(conn, %{"id" => plan_id}) do
    plan = Billing.get_plan!(plan_id)
    changeset = Billing.Plan.changeset(plan, %{})

    conn
    |> render("edit.html", changeset: changeset, plan: plan)
  end

  def update(conn, %{"plan" => %{"id" => id} = params}) do
    plan = Billing.get_plan!(id)

    case Billing.update_plan(plan, params) do
      {:ok, _plan} ->
        conn
        |> put_flash(:info, "Plan updated!")
        |> redirect(to: Routes.admin_plan_path(conn, :index))

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("edit.html", changeset: changeset, plan: plan)
    end
  end
end

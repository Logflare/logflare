defmodule LogflareWeb.UserController do
  use LogflareWeb, :controller

  alias Logflare.{User, Repo}
  alias Logflare.Google.BigQuery
  alias Logflare.Google.CloudResourceManager
  alias Logflare.Source.Supervisor

  @service_account Application.get_env(:logflare, Logflare.Google)[:service_account] || ""

  def edit(%{assigns: %{user: user}} = conn, _params) do
    changeset = User.update_by_user_changeset(user, %{})

    render(conn, "edit.html",
      changeset: changeset,
      user: user,
      service_account: @service_account
    )
  end

  def update(conn, %{"user" => params}) do
    user = conn.assigns.user
    prev_bigquery_project_id = user.bigquery_project_id
    prev_bigquery_dataset_location = user.bigquery_dataset_location

    user
    |> User.update_by_user_changeset(params)
    |> Repo.update()
    |> case do
      {:ok, user} ->
        new_bq_project? = user.bigquery_project_id != prev_bigquery_project_id
        new_bq_location? = user.bigquery_dataset_location != prev_bigquery_dataset_location

        if new_bq_project?, do: Supervisor.reset_all_user_tables(user)

        conn
        |> put_flash(:info, "Account updated!")
        |> put_flash(:new_bq_project, new_bq_project?)
        |> redirect(to: Routes.user_path(conn, :edit))

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> put_status(406)
        |> render("edit.html",
          changeset: changeset,
          user: conn.assigns.user,
          service_account: @service_account
        )
    end
  end

  def delete(%{assigns: %{user: user}} = conn, _params) do
    # TODO: soft delete, delayed deleted
    Repo.delete!(user)
    BigQuery.delete_dataset(user)

    spawn(fn ->
      CloudResourceManager.set_iam_policy()
    end)

    conn
    |> put_flash(:info, "Account deleted!")
    |> redirect(to: Routes.marketing_path(conn, :index))
  end
end

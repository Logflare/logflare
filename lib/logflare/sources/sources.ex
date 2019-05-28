defmodule Logflare.Sources do
  alias Logflare.{Repo, Source}
  alias Logflare.SourceRateCounter, as: SRC

  def get_metrics(sid, bucket: :default) when is_atom(sid) do
    SRC.get_metrics(sid, :default)
  end

  @spec get_by_name(binary()) ::
          nil | [%{optional(atom()) => any()}] | %{optional(atom()) => any()}
  def get_by_name(source_name) when is_binary(source_name) do
    Source
    |> Repo.get_by(name: source_name)
    |> Repo.preload(:user)
    |> Repo.preload(:rules)
  end

  def get_api_rate_by_id(sid, bucket: :default) do
    SRC.get_avg_rate(sid)
  end

  def get_by_id(source_id) when is_atom(source_id) do
    Source
    |> Repo.get_by(token: source_id)
    |> Repo.preload(:user)
    |> Repo.preload(:rules)
  end

  def get_by_public_token(public_token) when is_binary(public_token) do
    Source
    |> Repo.get_by(public_token: public_token)
    |> preload_defaults()
  end

  def get_by_pk(pk) when is_binary(pk) do
    pk
    |> String.to_integer()
    |> get_by_pk()
  end

  def get_by_pk(pk) when is_integer(pk) do
    Source
    |> Repo.get(pk)
    |> preload_defaults()
  end

  def preload_defaults(source) do
    source
    |> Repo.preload(:user)
    |> Repo.preload(:rules)
  end
end

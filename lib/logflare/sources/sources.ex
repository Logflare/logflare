defmodule Logflare.Sources do
  alias Logflare.{Repo, Source}
  alias Logflare.SourceRateCounter, as: SRC

  def get_by(kw) do
    Source
    |> Repo.get_by(kw)
    |> preload_defaults()
  end

  def get_metrics(source, bucket: :default) do
    SRC.get_metrics(source.token, :default)
  end

  def get_api_rate(source, bucket: :default) do
    SRC.get_avg_rate(source.token)
  end

  def preload_defaults(source) do
    source
    |> Repo.preload(:user)
    |> Repo.preload(:rules)
  end
end

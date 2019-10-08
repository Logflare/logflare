defmodule Logflare.Users.API do
  require Logger

  @type api_rates_quotas :: %{
          message: String.t(),
          metrics: %{
            user: %{
              remaining: integer(),
              limit: integer()
            },
            source: %{
              remaining: integer(),
              limit: integer()
            }
          }
        }

  defmodule Cache do
    @moduledoc """
    Caches API rate data from external cluster store
    """
    @ttl 1_000
    import Cachex.Spec
    @cache __MODULE__

    def child_spec(_) do
      %{
        id: @cache,
        start: {
          Cachex,
          :start_link,
          [
            @cache,
            [expiration: expiration(default: @ttl)]
          ]
        }
      }
    end

    def get_ingest_rates(source) do
      %{
        source_rate: get({:rate, :source, source.token}, 0),
        user_rate: get({:rate, :user, source.user.id}, 0)
      }
    end

    def put_user_rate(user, rate) do
      Cachex.put(@cache, {:rate, :user, user.id}, rate)
    end

    def put_source_rate(source, rate) do
      Cachex.put(@cache, {:rate, :source, source.token}, rate)
    end

    def get(key, default) do
      case Cachex.get(@cache, key) do
        {:ok, nil} ->
          default

        {:ok, value} ->
          value

        {:error, error} = errtup ->
          Logger.error("Cachex error: #{inspect(error)}")
          errtup
      end
    end
  end

  @source_rate_message "Source rate is over the API quota. Email support@logflare.app to increase your rate limit."
  @user_rate_message "User rate is over the API quota. Email support@logflare.app to increase your rate limit."

  @type ok_err_tup :: {:ok, api_rates_quotas} | {:error, api_rates_quotas}

  @callback verify_api_rates_quotas(map) :: ok_err_tup

  alias Logflare.{Sources, User}
  @api_call_logs {:api_call, :logs_post}

  @duration 60

  @spec verify_api_rates_quotas(map) :: ok_err_tup
  def verify_api_rates_quotas(%{type: @api_call_logs} = action) do
    %{source: source, user: user} = action

    %{
      source_rate: source_rate,
      user_rate: user_rate
    } = Cache.get_ingest_rates(source)

    source_limit = @duration * source.api_quota
    source_remaining = source_limit - source_rate

    user_limit = @duration * user.api_quota
    user_remaining = user_limit - user_rate

    {status, message} =
      cond do
        source_remaining <= 0 ->
          {:error, @source_rate_message}

        user_remaining <= 0 ->
          {:error, @user_rate_message}

        source_remaining > 0 and user_remaining > 0 ->
          {:ok, nil}
      end

    metrics_message = %{
      message: message,
      metrics: %{
        user: %{
          remaining: user_remaining,
          limit: user_limit
        },
        source: %{
          remaining: source_remaining,
          limit: source_limit
        }
      }
    }

    {status, metrics_message}
  end
end

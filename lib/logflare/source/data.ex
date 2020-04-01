defmodule Logflare.Source.Data do
  @moduledoc false
  alias Logflare.Sources.Counters
  alias Logflare.Cluster
  alias Logflare.Google.BigQuery
  alias Logflare.Source.{RateCounterServer, BigQuery.Buffer, BigQuery.Schema}

  @spec get_log_count(atom, String.t()) :: non_neg_integer()
  def get_log_count(token, _bigquery_project_id) do
    case BigQuery.get_table(token) do
      {:ok, table_info} ->
        table_rows = String.to_integer(table_info.numRows)

        buffer_rows =
          case is_nil(table_info.streamingBuffer) do
            true ->
              0

            false ->
              case is_nil(table_info.streamingBuffer.estimatedRows) do
                true ->
                  0

                false ->
                  String.to_integer(table_info.streamingBuffer.estimatedRows)
              end
          end

        table_rows + buffer_rows

      {:error, _message} ->
        0
    end
  end

  @spec get_ets_count(atom) :: non_neg_integer
  def get_ets_count(source_id) when is_atom(source_id) do
    log_table_info = :ets.info(source_id)

    case log_table_info do
      :undefined ->
        0

      _ ->
        log_table_info[:size]
    end
  end

  @spec get_total_inserts(atom) :: non_neg_integer
  def get_total_inserts(source_id) when is_atom(source_id) do
    log_table_info = :ets.info(source_id)

    case log_table_info do
      :undefined ->
        0

      _ ->
        get_node_inserts(source_id) + get_bq_inserts(source_id)
    end
  end

  def get_node_inserts(source_id) do
    {:ok, inserts} = Counters.get_inserts(source_id)
    inserts
  end

  def get_bq_inserts(source_id) do
    {:ok, bq_inserts} = Counters.get_bq_inserts(source_id)
    bq_inserts
  end

  def get_inserts(source_id) when is_atom(source_id) do
    log_table_info = :ets.info(source_id)

    case log_table_info do
      :undefined ->
        0

      _ ->
        {:ok, inserts} = Counters.get_inserts(source_id)
        inserts
    end
  end

  @spec get_rate(map) :: non_neg_integer
  def get_rate(%{id: _id, name: _name, token: source_token}) do
    {:ok, source_id} = Ecto.UUID.Atom.load(source_token)

    get_rate_int(source_id)
  end

  @spec get_rate(atom) :: non_neg_integer
  def get_rate(source_id) when is_atom(source_id) do
    get_rate_int(source_id)
  end

  @spec get_logs(atom) :: list(term)
  def get_logs(source_id) when is_atom(source_id) do
    case :ets.info(source_id) do
      :undefined ->
        []

      _ ->
        List.flatten(:ets.match(source_id, {:_, :"$1"}))
    end
  end

  def get_logs_across_cluster(source_id) when is_atom(source_id) do
    nodes = Cluster.Utils.node_list_all()

    task =
      Task.async(fn ->
        for n <- nodes do
          Task.Supervisor.async(
            {Logflare.TaskSupervisor, n},
            __MODULE__,
            :get_logs,
            [source_id]
          )
        end
        |> Task.yield_many()
        |> Enum.map(fn {%Task{pid: pid}, res} ->
          res || Task.Supervisor.terminate_child(Logflare.TaskSupervisor, pid)
        end)
      end)

    case Task.yield(task, 5_000) || Task.shutdown(task) do
      {:ok, results} ->
        for {:ok, events} <- results do
          events
        end
        |> List.flatten()
        |> Enum.sort_by(& &1.body.timestamp, &<=/2)
        |> Enum.take(-100)

      _else ->
        get_logs(source_id)
    end
  end

  @spec get_avg_rate(map) :: non_neg_integer
  def get_avg_rate(%{id: _id, name: _name, token: source_token}) do
    {:ok, source_id} = Ecto.UUID.Atom.load(source_token)

    case :ets.info(source_id) do
      :undefined ->
        0

      _ ->
        RateCounterServer.get_avg_rate(source_id)
    end
  end

  @spec get_avg_rate(atom) :: non_neg_integer
  def get_avg_rate(source_id) when is_atom(source_id) do
    case :ets.info(source_id) do
      :undefined ->
        0

      _ ->
        RateCounterServer.get_avg_rate(source_id)
    end
  end

  @spec get_max_rate(atom) :: integer
  def get_max_rate(source_id) do
    case :ets.info(source_id) do
      :undefined ->
        0

      _ ->
        RateCounterServer.get_max_rate(source_id)
    end
  end

  @spec get_latest_date(atom) :: any
  def get_latest_date(source_id, fallback \\ 0) when is_atom(source_id) do
    case :ets.info(source_id) do
      :undefined ->
        fallback

      _ ->
        case :ets.last(source_id) do
          :"$end_of_table" ->
            fallback

          {timestamp, _unique_int, _monotime} ->
            timestamp
        end
    end
  end

  @spec get_buffer(atom, integer) :: integer
  def get_buffer(source_id, fallback \\ 0) do
    case Process.whereis(String.to_atom("#{source_id}-buffer")) do
      nil ->
        fallback

      _ ->
        Buffer.get_count(source_id)
    end
  end

  @spec get_schema_field_count(struct()) :: non_neg_integer
  def get_schema_field_count(source) do
    case Process.whereis(String.to_atom("#{source.token}-schema")) do
      nil ->
        0

      _ ->
        Schema.get_state(source.token).field_count
    end
  end

  @spec get_rate_int(atom) :: non_neg_integer
  defp get_rate_int(source_id) do
    case :ets.info(source_id) do
      :undefined ->
        0

      _ ->
        RateCounterServer.get_rate(source_id)
    end
  end
end

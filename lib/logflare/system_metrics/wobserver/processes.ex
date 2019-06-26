defmodule Logflare.SystemMetrics.Wobserver.Processes do
  @moduledoc """
  Process and pid handling.
  via https://github.com/epinault/wobserver/blob/master/lib/wobserver/util/process.ex
  """

  import Logflare.SystemMetrics.Wobserver.Helper, only: [string_to_module: 1, format_function: 1]

  @process_summary [
    :registered_name,
    :initial_call,
    :memory,
    :reductions,
    :current_function,
    :message_queue_len,
    :dictionary
  ]

  @process_full [
    :registered_name,
    :priority,
    :trap_exit,
    :initial_call,
    :current_function,
    :message_queue_len,
    :error_handler,
    :group_leader,
    :links,
    :memory,
    :total_heap_size,
    :heap_size,
    :stack_size,
    :min_heap_size,
    :garbage_collection,
    :status,
    :dictionary
  ]

  @process_meta [
    :initial_call,
    :current_function,
    :status,
    :dictionary
  ]

  @doc """
  Turns the argument `pid` into a pid or if not possible returns `nil`.
  It will accept:
    - pids
    - atoms / module names (registered processes)
    - single integers
    - a list of 3 integers
    - a tuple of 3 integers
    - a charlist in the format: `'<0.0.0>'`
    - a String in the following formats:
        - `"#PID<0.0.0>"`
        - `"<0.0.0>"`
        - atom / module name
  Example:
  ```bash
  iex> Wobserver.Util.Process.pid pid(0, 33, 0)
  #PID<0.33.0>
  ```
  ```bash
  iex> Wobserver.Util.Process.pid :cowboy_sup
  #PID<0.253.0>
  ```
  ```bash
  iex> Wobserver.Util.Process.pid Logger
  #PID<0.213.0>
  ```
  ```bash
  iex> Wobserver.Util.Process.pid 33
  #PID<0.33.0>
  ```
  ```bash
  iex> Wobserver.Util.Process.pid [0, 33, 0]
  #PID<0.33.0>
  ```
  ```bash
  iex> Wobserver.Util.Process.pid '<0.33.0>'
  #PID<0.33.0>
  ```
  ```bash
  iex> Wobserver.Util.Process.pid {0, 33, 0}
  #PID<0.33.0>
  ```
  ```bash
  iex> Wobserver.Util.Process.pid "#PID<0.33.0>"
  #PID<0.33.0>
  ```
  ```bash
  iex> Wobserver.Util.Process.pid "<0.33.0>"
  #PID<0.33.0>
  ```
  ```bash
  iex> Wobserver.Util.Process.pid "cowboy_sup"
  #PID<0.253.0>
  ```
  ```bash
  iex> Wobserver.Util.Process.pid "Logger"
  #PID<0.213.0>
  ```
  ```bash
  iex> Wobserver.Util.Process.pid 4.5
  nil
  ```
  """
  @spec pid(pid :: pid | atom | list | binary | integer | {integer, integer, integer}) ::
          pid | nil
  def pid(pid)

  def pid(pid) when is_pid(pid), do: pid
  def pid(pid) when is_atom(pid), do: Process.whereis(pid)
  def pid(pid) when is_integer(pid), do: pid("<0.#{pid}.0>")
  def pid([a, b, c]), do: pid("<#{a}.#{b}.#{c}>")
  def pid(pid) when is_list(pid), do: :erlang.list_to_pid(pid)
  def pid({a, b, c}), do: pid("<#{a}.#{b}.#{c}>")
  def pid("#PID" <> pid), do: pid |> String.to_charlist() |> pid()
  def pid(pid = "<" <> _), do: pid |> String.to_charlist() |> pid()
  def pid(pid) when is_binary(pid), do: pid |> string_to_module() |> pid()
  def pid(_), do: nil

  @doc """
  Turns the argument `pid` into a pid or if not possible raises error.
  For example see: `Wobserver.Util.Process.pid/1`.
  """
  @spec pid!(pid :: pid | list | binary | integer | {integer, integer, integer}) :: pid
  def pid!(pid) do
    case pid(pid) do
      nil ->
        raise ArgumentError, message: "Can not convert #{inspect(pid)} to pid."

      p ->
        p
    end
  end

  @doc """
  Creates a complete overview of process stats based on the given `pid`.
  Including but not limited to:
    - `id`, the process pid
    - `name`, the registered name or `nil`.
    - `init`, initial function or name.
    - `current`, current function.
    - `memory`, the total amount of memory used by the process.
    - `reductions`, the amount of reductions.
    - `message_queue_length`, the amount of unprocessed messages for the process.,
  """
  @spec info(pid :: pid | list | binary | integer | {integer, integer, integer}) :: :error | map
  def info(pid) do
    pid
    |> pid()
    |> process_info(@process_full, &structure_full/2)
  end

  @doc """
  Retreives a list of process summaries.
  Every summary contains:
    - `id`, the process pid.
    - `name`, the registered name or `nil`.
    - `init`, initial function or name.
    - `current`, current function.
    - `memory`, the total amount of memory used by the process.
    - `reductions`, the amount of reductions.
    - `message_queue_length`, the amount of unprocessed messages for the process.
  """
  @spec list :: list(map)
  def list do
    :erlang.processes()
    |> Enum.map(&summary/1)
  end

  @doc """
  Creates formatted meta information about the process based on the given `pid`.
  The information contains:
    - `init`, initial function or name.
    - `current`, current function.
    - `status`, process status.
  """
  @spec meta(pid :: pid) :: map
  def meta(pid),
    do: pid |> process_info(@process_meta, &structure_meta/2)

  @doc """
  Creates formatted summary about the process based on the given `pid`.
  Every summary contains:
    - `id`, the process pid.
    - `name`, the registered name or `nil`.
    - `init`, initial function or name.
    - `current`, current function.
    - `memory`, the total amount of memory used by the process.
    - `reductions`, the amount of reductions.
    - `message_queue_length`, the amount of unprocessed messages for the process.
  """
  @spec summary(pid :: pid) :: map
  def summary(pid),
    do: pid |> process_info(@process_summary, &structure_summary/2)

  # Helpers

  defp process_info(nil, _, _), do: :error

  defp process_info(pid, information, structurer) do
    case :erlang.process_info(pid, information) do
      :undefined -> :error
      data -> structurer.(data, pid)
    end
  end

  defp process_status_module(pid) do
    {:status, ^pid, {:module, class}, _} = :sys.get_status(pid, 100)
    class
  catch
    _, _ -> :unknown
  end

  defp state(pid) do
    :sys.get_state(pid, 100)
  catch
    _, _ -> :unknown
  end

  @doc false
  @spec initial_call(data :: keyword) :: {atom, atom, integer} | atom
  def initial_call(data) do
    dictionary_init =
      data
      |> Keyword.get(:dictionary, [])
      |> Keyword.get(:"$initial_call", nil)

    case dictionary_init do
      nil ->
        Keyword.get(data, :initial_call, nil)

      call ->
        call
    end
  end

  # Structurers

  defp structure_summary(data, pid) do
    process_name =
      case Keyword.get(data, :registered_name, []) do
        [] -> nil
        name -> name
      end

    %{
      pid: pid,
      name: process_name,
      init: format_function(initial_call(data)),
      current: format_function(Keyword.get(data, :current_function, nil)),
      memory: Keyword.get(data, :memory, 0),
      reductions: Keyword.get(data, :reductions, 0),
      message_queue_length: Keyword.get(data, :message_queue_len, 0)
    }
  end

  defp structure_full(data, pid) do
    gc = Keyword.get(data, :garbage_collection, [])
    dictionary = Keyword.get(data, :dictionary)

    %{
      pid: pid,
      registered_name: Keyword.get(data, :registered_name, nil),
      priority: Keyword.get(data, :priority, :normal),
      trap_exit: Keyword.get(data, :trap_exit, false),
      message_queue_len: Keyword.get(data, :message_queue_len, 0),
      error_handler: Keyword.get(data, :error_handler, :none),
      relations: %{
        group_leader: Keyword.get(data, :group_leader, nil),
        ancestors: Keyword.get(dictionary, :"$ancestors", []),
        links: Keyword.get(data, :links, nil)
      },
      memory: %{
        total: Keyword.get(data, :memory, 0),
        stack_and_heap: Keyword.get(data, :total_heap_size, 0),
        heap_size: Keyword.get(data, :heap_size, 0),
        stack_size: Keyword.get(data, :stack_size, 0),
        gc_min_heap_size: Keyword.get(gc, :min_heap_size, 0),
        gc_full_sweep_after: Keyword.get(gc, :fullsweep_after, 0)
      },
      meta: structure_meta(data, pid),
      state: to_string(:io_lib.format("~tp", [state(pid)]))
    }
  end

  defp structure_meta(data, pid) do
    init = initial_call(data)

    class =
      case init do
        {:supervisor, _, _} -> :supervisor
        {:application_master, _, _} -> :application
        _ -> process_status_module(pid)
      end

    %{
      init: format_function(init),
      current: format_function(Keyword.get(data, :current_function)),
      status: Keyword.get(data, :status),
      class: class
    }
  end
end

defmodule Logflare.MemoryRepo.Migrations do
  use Logflare.Commons
  alias Logflare.Changefeeds.ChangefeedSubscription
  alias Logflare.EctoSchemaReflection
  use GenServer

  def start_link(args \\ %{}, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init(args) do
    run()
    {:ok, args}
  end

  def run() do
    create_or_replace_pg_jsonb_object_changes!()
    create_or_replace_pg_changefeed_notify!()
    create_or_replace_pg_changefeed_notify_id_only!()
          Changefeeds.list_changefeed_subscriptions() do
      # delete_table_for_schema(schema)
      create_table_for_schema(schema)
    end

    for {table, schema} <- Changefeeds.tables() do
      create_table_for_schema(schema)
    end

    for %{schema: schema} <- Changefeeds.list_changefeed_subscriptions() do
      create_table_for_schema_virtual_fields(schema)
    end
  end

  def delete_table_for_schema(schema) do
    :mnesia.delete_table(EctoSchemaReflection.source(schema))
  end

  def create_table_for_schema(schema) do
    attributes =
      EctoSchemaReflection.fields_no_embeds(schema) ++
        EctoSchemaReflection.embeds(schema)

    table = schema |> EctoSchemaReflection.source() |> String.to_atom()

    {:atomic, :ok} =
      :mnesia.create_table(table,
        ram_copies: [node()],
        record_name: schema,
        attributes: attributes,
        type: :ordered_set
      )
  end

  def create_table_for_schema_virtual_fields(schema) do
    attributes = EctoSchemaReflection.virtual_fields(schema)
    virtual_schema = Module.concat(schema, Virtual)

    if not Enum.empty?(attributes) do
      table = :"#{EctoSchemaReflection.source(schema)}_virtual"

      {:atomic, :ok} =
        :mnesia.create_table(table,
          ram_copies: [node()],
          record_name: virtual_schema,
          attributes: attributes ++ [:id],
          type: :ordered_set
        )
    end
  end

  def create_or_replace_pg_jsonb_object_changes!() do
    Repo.query!(
      """
        CREATE OR REPLACE FUNCTION jsonb_object_changes(old jsonb, new jsonb)
          RETURNS jsonb
          LANGUAGE sql
          IMMUTABLE
          STRICT
        AS
        $$
        SELECT json_object_agg(key, new -> key)
        FROM jsonb_object_keys(jsonb_concat(old, new)) AS key
        WHERE old -> key <> new -> key
          OR new -> key IS NULL
          OR old -> key IS NULL
        $$;
      """,
      [],
      log: false
    )
  end

  def create_or_replace_pg_changefeed_notify!() do
    Repo.query!(
      """
        CREATE OR REPLACE FUNCTION changefeed_notify()
          RETURNS trigger AS
        $$
        DECLARE
          current_row RECORD;
          changes     jsonb;
        BEGIN
          CASE
            WHEN tg_op = 'INSERT' THEN
              current_row := new;
              changes := to_jsonb(current_row);
            WHEN tg_op = 'UPDATE' THEN
              current_row := new;
              changes := jsonb_object_changes(to_jsonb(old), to_jsonb(new));
            WHEN tg_op = 'DELETE' THEN
              current_row := old;
              changes = 'null'::jsonb;
            ELSE
              RAISE NOTICE 'TG_OP should never be anything then INSERT, UPDATE or DELETE';
            END CASE;
          PERFORM pg_notify(
              tg_table_name || '_changefeed',
              json_build_object(
                  'table', tg_table_name,
                  'type', tg_op,
                  'id', current_row.id,
                  'changes', changes
                )::text
            );
          RETURN current_row;
        END;
        $$ LANGUAGE plpgsql;
      """,
      [],
      log: false
    )
  end

  def create_or_replace_pg_changefeed_notify_id_only!() do
    Repo.query!(
      """
      CREATE OR REPLACE FUNCTION changefeed_id_only_notify()
      RETURNS trigger AS
      $$
      DECLARE
          current_row RECORD;
      BEGIN
          IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
              current_row := NEW;
          ELSE
              current_row := OLD;
          END IF;
          PERFORM pg_notify(
                  TG_TABLE_NAME || '_id_only_changefeed',
                  json_build_object(
                          'table', TG_TABLE_NAME,
                          'type', TG_OP,
                          'id', current_row.id
                      )::text
              );
          RETURN current_row;
      END;
      $$ LANGUAGE plpgsql;
      """,
      [],
      log: false
    )
  end

  def create_changefeed_trigger(%ChangefeedSubscription{id_only: id_only, table: table}) do
    trigger = "#{table}_changefeed_trigger"

    trigger =
      if id_only do
        trigger <> "_id_only"
      else
        trigger
      end

    Repo.query!(
      """
      DO $$
      BEGIN
        IF NOT EXISTS(SELECT *
          FROM information_schema.triggers
          WHERE event_object_table = '#{table}'
          AND trigger_name = '#{trigger}'
        )
        THEN
        CREATE TRIGGER #{trigger}
            AFTER INSERT OR UPDATE OR DELETE
            ON #{table}
                FOR EACH ROW
        EXECUTE PROCEDURE changefeed_notify();
        END IF;
      END;
      $$

      """,
      [],
      log: false
    )
  end
end

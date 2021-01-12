defmodule Logflare.EctoChangesetExtras do
  alias Logflare.EctoSchemaReflection

  def cast_all_fields(struct, attrs) do
    Ecto.Changeset.cast(struct, attrs, EctoSchemaReflection.fields_no_embeds(struct.__struct__))
  end
end

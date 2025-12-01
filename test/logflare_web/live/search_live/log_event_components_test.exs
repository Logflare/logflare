 defmodule LogflareWeb.SearchLive.LogEventComponentsTest do
   use LogflareWeb.ConnCase, async: true

   import Phoenix.LiveViewTest
  import Phoenix.Component

   alias LogflareWeb.SearchLive.LogEventComponents

   describe "selected_fields/1" do
     test "renders selected fields with display names and values" do
       log_event =
         build(:log_event,
           metadata_user_id: "user_123",
           metadata_store_city: "San Francisco",
           food: "Pizza"
         )

       select_fields = [
         %{display: "metadata.user_id", key: "metadata_user_id"},
         %{display: "metadata.store.city", key: "metadata_store_city"},
         %{display: "food", key: "food"}
       ]

       assigns = %{
         log_event: log_event,
         select_fields: select_fields
       }

       html =
         rendered_to_string(~H"""
         <LogEventComponents.selected_fields log_event={@log_event} select_fields={@select_fields} />
         """)

       assert html =~ "metadata.user_id:"
       assert html =~ "user_123"
       assert html =~ "metadata.store.city:"
       assert html =~ "San Francisco"
       assert html =~ "food:"
       assert html =~ "Pizza"
     end

     test "handles null values" do
       log_event = build(:log_event, metadata_user_id: nil)

       select_fields = [
         %{display: "metadata.user_id", key: "metadata_user_id"}
       ]

       assigns = %{
         log_event: log_event,
         select_fields: select_fields
       }

       html =
         rendered_to_string(~H"""
         <LogEventComponents.selected_fields log_event={@log_event} select_fields={@select_fields} />
         """)

       assert html =~ "metadata.user_id:"
       assert html =~ "null"
     end
   end
 end

defmodule Google.Cloud.Bigquery.Reservation.V1.Edition do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :EDITION_UNSPECIFIED, 0
  field :STANDARD, 1
  field :ENTERPRISE, 2
  field :ENTERPRISE_PLUS, 3
end

defmodule Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment.CommitmentPlan do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :COMMITMENT_PLAN_UNSPECIFIED, 0
  field :FLEX, 3
  field :FLEX_FLAT_RATE, 7
  field :TRIAL, 5
  field :MONTHLY, 2
  field :MONTHLY_FLAT_RATE, 8
  field :ANNUAL, 4
  field :ANNUAL_FLAT_RATE, 9
  field :THREE_YEAR, 10
  field :NONE, 6
end

defmodule Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment.State do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :STATE_UNSPECIFIED, 0
  field :PENDING, 1
  field :ACTIVE, 2
  field :FAILED, 3
end

defmodule Google.Cloud.Bigquery.Reservation.V1.Assignment.JobType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :JOB_TYPE_UNSPECIFIED, 0
  field :PIPELINE, 1
  field :QUERY, 2
  field :ML_EXTERNAL, 3
  field :BACKGROUND, 4
  field :CONTINUOUS, 6
end

defmodule Google.Cloud.Bigquery.Reservation.V1.Assignment.State do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :STATE_UNSPECIFIED, 0
  field :PENDING, 1
  field :ACTIVE, 2
end

defmodule Google.Cloud.Bigquery.Reservation.V1.Reservation.Autoscale do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :current_slots, 1, type: :int64, json_name: "currentSlots", deprecated: false
  field :max_slots, 2, type: :int64, json_name: "maxSlots"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.Reservation.ReplicationStatus do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :error, 1, type: Google.Rpc.Status, deprecated: false

  field :last_error_time, 2,
    type: Google.Protobuf.Timestamp,
    json_name: "lastErrorTime",
    deprecated: false

  field :last_replication_time, 3,
    type: Google.Protobuf.Timestamp,
    json_name: "lastReplicationTime",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Reservation.V1.Reservation do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string
  field :slot_capacity, 2, type: :int64, json_name: "slotCapacity"
  field :ignore_idle_slots, 4, type: :bool, json_name: "ignoreIdleSlots"
  field :autoscale, 7, type: Google.Cloud.Bigquery.Reservation.V1.Reservation.Autoscale
  field :concurrency, 16, type: :int64

  field :creation_time, 8,
    type: Google.Protobuf.Timestamp,
    json_name: "creationTime",
    deprecated: false

  field :update_time, 9,
    type: Google.Protobuf.Timestamp,
    json_name: "updateTime",
    deprecated: false

  field :multi_region_auxiliary, 14, type: :bool, json_name: "multiRegionAuxiliary"
  field :edition, 17, type: Google.Cloud.Bigquery.Reservation.V1.Edition, enum: true
  field :primary_location, 18, type: :string, json_name: "primaryLocation", deprecated: false
  field :secondary_location, 19, type: :string, json_name: "secondaryLocation", deprecated: false

  field :original_primary_location, 20,
    type: :string,
    json_name: "originalPrimaryLocation",
    deprecated: false

  field :replication_status, 24,
    type: Google.Cloud.Bigquery.Reservation.V1.Reservation.ReplicationStatus,
    json_name: "replicationStatus",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :slot_count, 2, type: :int64, json_name: "slotCount"

  field :plan, 3,
    type: Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment.CommitmentPlan,
    enum: true

  field :state, 4,
    type: Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment.State,
    enum: true,
    deprecated: false

  field :commitment_start_time, 9,
    type: Google.Protobuf.Timestamp,
    json_name: "commitmentStartTime",
    deprecated: false

  field :commitment_end_time, 5,
    type: Google.Protobuf.Timestamp,
    json_name: "commitmentEndTime",
    deprecated: false

  field :failure_status, 7, type: Google.Rpc.Status, json_name: "failureStatus", deprecated: false

  field :renewal_plan, 8,
    type: Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment.CommitmentPlan,
    json_name: "renewalPlan",
    enum: true

  field :multi_region_auxiliary, 10, type: :bool, json_name: "multiRegionAuxiliary"
  field :edition, 12, type: Google.Cloud.Bigquery.Reservation.V1.Edition, enum: true
  field :is_flat_rate, 14, type: :bool, json_name: "isFlatRate", deprecated: false
end

defmodule Google.Cloud.Bigquery.Reservation.V1.CreateReservationRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :reservation_id, 2, type: :string, json_name: "reservationId"
  field :reservation, 3, type: Google.Cloud.Bigquery.Reservation.V1.Reservation
end

defmodule Google.Cloud.Bigquery.Reservation.V1.ListReservationsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :page_size, 2, type: :int32, json_name: "pageSize"
  field :page_token, 3, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.ListReservationsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :reservations, 1, repeated: true, type: Google.Cloud.Bigquery.Reservation.V1.Reservation
  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.GetReservationRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Reservation.V1.DeleteReservationRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Reservation.V1.UpdateReservationRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :reservation, 1, type: Google.Cloud.Bigquery.Reservation.V1.Reservation
  field :update_mask, 2, type: Google.Protobuf.FieldMask, json_name: "updateMask"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.FailoverReservationRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Reservation.V1.CreateCapacityCommitmentRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :capacity_commitment, 2,
    type: Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment,
    json_name: "capacityCommitment"

  field :enforce_single_admin_project_per_org, 4,
    type: :bool,
    json_name: "enforceSingleAdminProjectPerOrg"

  field :capacity_commitment_id, 5, type: :string, json_name: "capacityCommitmentId"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.ListCapacityCommitmentsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :page_size, 2, type: :int32, json_name: "pageSize"
  field :page_token, 3, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.ListCapacityCommitmentsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :capacity_commitments, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment,
    json_name: "capacityCommitments"

  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.GetCapacityCommitmentRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Reservation.V1.DeleteCapacityCommitmentRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :force, 3, type: :bool
end

defmodule Google.Cloud.Bigquery.Reservation.V1.UpdateCapacityCommitmentRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :capacity_commitment, 1,
    type: Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment,
    json_name: "capacityCommitment"

  field :update_mask, 2, type: Google.Protobuf.FieldMask, json_name: "updateMask"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.SplitCapacityCommitmentRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :slot_count, 2, type: :int64, json_name: "slotCount"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.SplitCapacityCommitmentResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :first, 1, type: Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment
  field :second, 2, type: Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment
end

defmodule Google.Cloud.Bigquery.Reservation.V1.MergeCapacityCommitmentsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :capacity_commitment_ids, 2,
    repeated: true,
    type: :string,
    json_name: "capacityCommitmentIds"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.Assignment do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :assignee, 4, type: :string

  field :job_type, 3,
    type: Google.Cloud.Bigquery.Reservation.V1.Assignment.JobType,
    json_name: "jobType",
    enum: true

  field :state, 6,
    type: Google.Cloud.Bigquery.Reservation.V1.Assignment.State,
    enum: true,
    deprecated: false

  field :enable_gemini_in_bigquery, 10,
    type: :bool,
    json_name: "enableGeminiInBigquery",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Reservation.V1.CreateAssignmentRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :assignment, 2, type: Google.Cloud.Bigquery.Reservation.V1.Assignment
  field :assignment_id, 4, type: :string, json_name: "assignmentId"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.ListAssignmentsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :page_size, 2, type: :int32, json_name: "pageSize"
  field :page_token, 3, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.ListAssignmentsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :assignments, 1, repeated: true, type: Google.Cloud.Bigquery.Reservation.V1.Assignment
  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.DeleteAssignmentRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Reservation.V1.SearchAssignmentsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :query, 2, type: :string
  field :page_size, 3, type: :int32, json_name: "pageSize"
  field :page_token, 4, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.SearchAllAssignmentsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :query, 2, type: :string
  field :page_size, 3, type: :int32, json_name: "pageSize"
  field :page_token, 4, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.SearchAssignmentsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :assignments, 1, repeated: true, type: Google.Cloud.Bigquery.Reservation.V1.Assignment
  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.SearchAllAssignmentsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :assignments, 1, repeated: true, type: Google.Cloud.Bigquery.Reservation.V1.Assignment
  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.MoveAssignmentRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :destination_id, 3, type: :string, json_name: "destinationId", deprecated: false
  field :assignment_id, 5, type: :string, json_name: "assignmentId"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.UpdateAssignmentRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :assignment, 1, type: Google.Cloud.Bigquery.Reservation.V1.Assignment
  field :update_mask, 2, type: Google.Protobuf.FieldMask, json_name: "updateMask"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.TableReference do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId"
  field :dataset_id, 2, type: :string, json_name: "datasetId"
  field :table_id, 3, type: :string, json_name: "tableId"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.BiReservation do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string

  field :update_time, 3,
    type: Google.Protobuf.Timestamp,
    json_name: "updateTime",
    deprecated: false

  field :size, 4, type: :int64

  field :preferred_tables, 5,
    repeated: true,
    type: Google.Cloud.Bigquery.Reservation.V1.TableReference,
    json_name: "preferredTables"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.GetBiReservationRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Reservation.V1.UpdateBiReservationRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :bi_reservation, 1,
    type: Google.Cloud.Bigquery.Reservation.V1.BiReservation,
    json_name: "biReservation"

  field :update_mask, 2, type: Google.Protobuf.FieldMask, json_name: "updateMask"
end

defmodule Google.Cloud.Bigquery.Reservation.V1.ReservationService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.reservation.v1.ReservationService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :CreateReservation,
    Google.Cloud.Bigquery.Reservation.V1.CreateReservationRequest,
    Google.Cloud.Bigquery.Reservation.V1.Reservation
  )

  rpc(
    :ListReservations,
    Google.Cloud.Bigquery.Reservation.V1.ListReservationsRequest,
    Google.Cloud.Bigquery.Reservation.V1.ListReservationsResponse
  )

  rpc(
    :GetReservation,
    Google.Cloud.Bigquery.Reservation.V1.GetReservationRequest,
    Google.Cloud.Bigquery.Reservation.V1.Reservation
  )

  rpc(
    :DeleteReservation,
    Google.Cloud.Bigquery.Reservation.V1.DeleteReservationRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :UpdateReservation,
    Google.Cloud.Bigquery.Reservation.V1.UpdateReservationRequest,
    Google.Cloud.Bigquery.Reservation.V1.Reservation
  )

  rpc(
    :FailoverReservation,
    Google.Cloud.Bigquery.Reservation.V1.FailoverReservationRequest,
    Google.Cloud.Bigquery.Reservation.V1.Reservation
  )

  rpc(
    :CreateCapacityCommitment,
    Google.Cloud.Bigquery.Reservation.V1.CreateCapacityCommitmentRequest,
    Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment
  )

  rpc(
    :ListCapacityCommitments,
    Google.Cloud.Bigquery.Reservation.V1.ListCapacityCommitmentsRequest,
    Google.Cloud.Bigquery.Reservation.V1.ListCapacityCommitmentsResponse
  )

  rpc(
    :GetCapacityCommitment,
    Google.Cloud.Bigquery.Reservation.V1.GetCapacityCommitmentRequest,
    Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment
  )

  rpc(
    :DeleteCapacityCommitment,
    Google.Cloud.Bigquery.Reservation.V1.DeleteCapacityCommitmentRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :UpdateCapacityCommitment,
    Google.Cloud.Bigquery.Reservation.V1.UpdateCapacityCommitmentRequest,
    Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment
  )

  rpc(
    :SplitCapacityCommitment,
    Google.Cloud.Bigquery.Reservation.V1.SplitCapacityCommitmentRequest,
    Google.Cloud.Bigquery.Reservation.V1.SplitCapacityCommitmentResponse
  )

  rpc(
    :MergeCapacityCommitments,
    Google.Cloud.Bigquery.Reservation.V1.MergeCapacityCommitmentsRequest,
    Google.Cloud.Bigquery.Reservation.V1.CapacityCommitment
  )

  rpc(
    :CreateAssignment,
    Google.Cloud.Bigquery.Reservation.V1.CreateAssignmentRequest,
    Google.Cloud.Bigquery.Reservation.V1.Assignment
  )

  rpc(
    :ListAssignments,
    Google.Cloud.Bigquery.Reservation.V1.ListAssignmentsRequest,
    Google.Cloud.Bigquery.Reservation.V1.ListAssignmentsResponse
  )

  rpc(
    :DeleteAssignment,
    Google.Cloud.Bigquery.Reservation.V1.DeleteAssignmentRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :SearchAssignments,
    Google.Cloud.Bigquery.Reservation.V1.SearchAssignmentsRequest,
    Google.Cloud.Bigquery.Reservation.V1.SearchAssignmentsResponse
  )

  rpc(
    :SearchAllAssignments,
    Google.Cloud.Bigquery.Reservation.V1.SearchAllAssignmentsRequest,
    Google.Cloud.Bigquery.Reservation.V1.SearchAllAssignmentsResponse
  )

  rpc(
    :MoveAssignment,
    Google.Cloud.Bigquery.Reservation.V1.MoveAssignmentRequest,
    Google.Cloud.Bigquery.Reservation.V1.Assignment
  )

  rpc(
    :UpdateAssignment,
    Google.Cloud.Bigquery.Reservation.V1.UpdateAssignmentRequest,
    Google.Cloud.Bigquery.Reservation.V1.Assignment
  )

  rpc(
    :GetBiReservation,
    Google.Cloud.Bigquery.Reservation.V1.GetBiReservationRequest,
    Google.Cloud.Bigquery.Reservation.V1.BiReservation
  )

  rpc(
    :UpdateBiReservation,
    Google.Cloud.Bigquery.Reservation.V1.UpdateBiReservationRequest,
    Google.Cloud.Bigquery.Reservation.V1.BiReservation
  )
end

defmodule Google.Cloud.Bigquery.Reservation.V1.ReservationService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.Reservation.V1.ReservationService.Service
end

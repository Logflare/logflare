require Logger
if System.get_env("PATH") =~ "google-cloud-sdk" do
  Logger.warn("Google cloud sdk not found on path. Please install at https://cloud.google.com/sdk/docs/install")
end


defmodule Scripts do
  def deploy do

  end
  def list_instance_groups do
    [_ | groups] = System.cmd("gcloud", ["compute", "instance-groups", "managed", "list"])
    |> elem(0)
    |> String.split("\n")
    |> Enum.map(fn line ->
      String.split(line, ~r/\s{2,}/)
    end)

    groups
    |> Enum.map(fn
    [name, location, scope, base_instance_name, size, target_size, instance_template, autoscaled]->
    %{
      name: name,
      location: location,
      scope: scope,
      base_instance_name: base_instance_name,
      size: Integer.parse(size) |> elem(0),
      target_size: Integer.parse(target_size) |> elem(0),
      instance_template: instance_template,
      autoscaled: autoscaled
    }
    _-> nil
    end)
    |> Enum.filter(fn v-> v end)
    |> Enum.filter(fn v-> v.size != 0 end)
  end
  def get_working_instance_group do
    groups =  list_instance_groups()
    if groups |> length() > 1 do
      Logger.warn("Multiple instance groups with running instances detected, defaulting to first")
    end
    groups |> hd()
  end
  def get_latest_container_image do

  end
  def create_similar_instance_template(previous_template_name, container_image) do
    last_index = previous_template_name |> String.at(-1) |> Integer.parse() |> elem(0)
    next_id = last_index + 1
    new_name = (previous_template_name |> String.slice(0, -1)  )<> Integer.to_string(next_id)
    command = """
    gcloud compute instance-templates create-with-container #{new_name} --project=logflare-232118 --machine-type=c2-standard-30 --network-interface=network=global,network-tier=PREMIUM --maintenance-policy=MIGRATE --provisioning-model=STANDARD --service-account=1074203751359-compute@developer.gserviceaccount.com --scopes=https://www.googleapis.com/auth/cloud-platform --tags=phoenix-http --container-image=#{container_image} --container-restart-policy=always --container-privileged --create-disk=auto-delete=yes,boot=yes,device-name=logflare-c2-16cpu-docker-global-cos89-13,image=projects/cos-cloud/global/images/cos-85-13310-1416-18,mode=rw,size=25,type=pd-ssd --no-shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring --labels=container-vm=cos-85-13310-1416-18
    """
    # |> String.to_char_list |> :os.cmd
  end

  def canary_deploy(instance_group) do
    """
    gcloud beta compute instance-groups managed rolling-action start-update #{instance_group} --project=logflare-232118 --type='proactive' --max-surge=1 --max-unavailable=0 --min-ready=300 --minimal-action='replace' --most-disruptive-allowed-action='replace' --replacement-method='substitute' --version=template=projects/logflare-232118/global/instanceTemplates/logflare-c2-30cpu-docker-global-cos89-6 --canary-version=template=projects/logflare-232118/global/instanceTemplates/logflare-c2-30cpu-docker-global-cos89-7,target-size=1 --zone=europe-west3-b

    """
  end
end

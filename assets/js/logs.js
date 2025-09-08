import $ from "jquery"

export async function applyToAllLogTimestamps(formatter) {
  const timestamps = $(".log-datestamp")
  for (let time of timestamps) {
    const datetime = formatter(timestampMs)
    $(time).text(datetime)
    const timestampMs = time.getAttribute("data-timestamp");
    const datetime = formatter(timestampMs);
    $(time).text(datetime);
  }
}

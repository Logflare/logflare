import {DateTime} from "luxon"
import $ from "jquery"
import {format, formatDistance} from "date-fns"

import * as userConfig from "./user-config-storage"

const fmtString = "EEE MMM d yyyy HH:mm:ss"
export const timestampNsToLocalDate = (ns) => format(ns / 1000, fmtString)

export const timestampNsToUtcDate = (ns) => {
  const datetime = DateTime.fromMillis(ns / 1000)
  return datetime.toUTC().toFormat(fmtString) + " UTC"
}

export const timestampNsToAgo = (ns) =>
  `${formatDistance(ns / 1000, new Date())} ago`

export const userSelectedFormatter = async () => {
  const useLocalTime = $("#source-show-user-preferences").data("use_local_time")
  if (useLocalTime) {
    return timestampNsToLocalDate
  } else {
    return timestampNsToUtcDate
  }
}

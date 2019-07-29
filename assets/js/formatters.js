import { DateTime } from "luxon"
import { format, formatDistance } from "date-fns"

import * as userConfig from "./user-config-storage"

export const timestampNsToLocalDate = ns =>
    format(ns / 1000, "EEE MMM d yyyy hh:mm:ssa")

export const timestampNsToUtcDate = ns => {
    const datetime = DateTime.fromMillis(ns / 1000)
    return datetime.toUTC().toFormat("EEE MMM d yyyy hh:mm:ssa") + " UTC"
}

export const timestampNsToAgo = ns =>
    `${formatDistance(ns / 1000, new Date())} ago`

export const userSelectedFormatter = async () => {
    const useLocalTime = await userConfig.useLocalTime()
    if (useLocalTime) {
        return timestampNsToLocalDate
    } else {
        return timestampNsToUtcDate
    }
}

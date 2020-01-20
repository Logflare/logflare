import * as localforage from "localforage"

const store = localforage.createInstance({
  name: "userConfig",
})

const USE_LOCAL_TIME_KEY = "useLocalTime"

const init = async () => {
  await store.setItem(USE_LOCAL_TIME_KEY, true)
}

init()

const flipUseLocalTime = async () => {
  const current = await store.getItem(USE_LOCAL_TIME_KEY)
  await store.setItem(USE_LOCAL_TIME_KEY, !current)
}

const useLocalTime = () => store.getItem(USE_LOCAL_TIME_KEY)

export { flipUseLocalTime, useLocalTime }

// source code from https://github.com/kidh0/jquery.idle/blob/master/vanilla.idle.js
const idle = function(options) {
    var defaults = {
        idle: 60000, // idle time in ms
        events: ["mousemove", "keydown", "mousedown", "touchstart"], // events that will trigger the idle resetter
        onIdle: function() {}, // callback function to be executed after idle time
        onActive: function() {}, // callback function to be executed after back form idleness
        onHide: function() {}, // callback function to be executed when window become hidden
        onShow: function() {}, // callback function to be executed when window become visible
        keepTracking: true, // set it to false of you want to track only once
        startAtIdle: false, // set it to true if you want to start in the idle state
        recurIdleCall: false,
    }
    var settings = extend({}, defaults, options)
    var idle = settings.startAtIdle
    var visible = !settings.startAtIdle
    var visibilityEvents = [
        "visibilitychange",
        "webkitvisibilitychange",
        "mozvisibilitychange",
        "msvisibilitychange",
    ]
    var lastId = null
    var resetTimeout, timeout

    // event to clear all idle events
    window.addEventListener("idle:stop", function(event) {
        bulkRemoveEventListener(window, settings.events)
        settings.keepTracking = false
        resetTimeout(lastId, settings)
    })

    var resetTimeout = function resetTimeout(id, settings) {
        if (idle) {
            idle = false
            settings.onActive.call()
        }
        clearTimeout(id)
        if (settings.keepTracking) {
            return timeout(settings)
        }
    }

    var timeout = function timeout(settings) {
        var timer = settings.recurIdleCall ? setInterval : setTimeout
        var id
        id = timer(function() {
            idle = true
            settings.onIdle.call()
        }, settings.idle)
        return id
    }

    return {
        start: function() {
            lastId = timeout(settings)
            bulkAddEventListener(window, settings.events, function(event) {
                lastId = resetTimeout(lastId, settings)
            })
            if (settings.onShow || settings.onHide) {
                bulkAddEventListener(document, visibilityEvents, function(
                    event
                ) {
                    if (
                        document.hidden ||
                        document.webkitHidden ||
                        document.mozHidden ||
                        document.msHidden
                    ) {
                        if (visible) {
                            visible = false
                            settings.onHide.call()
                        }
                    } else {
                        if (!visible) {
                            visible = true
                            settings.onShow.call()
                        }
                    }
                })
            }
        },
    }
}

var bulkAddEventListener = function bulkAddEventListener(
    object,
    events,
    callback
) {
    events.forEach(function(event) {
        object.addEventListener(event, function(event) {
            callback(event)
        })
    })
}

var bulkRemoveEventListener = function bulkRemoveEventListener(object, events) {
    events.forEach(function(event) {
        object.removeEventListener(event)
    })
}

// Thanks to http://youmightnotneedjquery.com/
var extend = function extend(out) {
    out = out || {}
    for (var i = 1; i < arguments.length; i++) {
        if (!arguments[i]) {
            continue
        }
        for (var key in arguments[i]) {
            if (arguments[i].hasOwnProperty(key)) {
                out[key] = arguments[i][key]
            }
        }
    }
    return out
}

export default idle

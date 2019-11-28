/* appear.js 1.2.1 */
"use strict"
var scrollLastPos = null, scrollTimer = 0, scroll = {}

function track() {
  var newPos = window.scrollY || window.pageYOffset  // pageYOffset for IE9
  if (scrollLastPos != null) {
    scroll.velocity = newPos - scrollLastPos
    scroll.delta = (scroll.velocity >= 0) ? scroll.velocity : (-1 * scroll.velocity)

  }
  scrollLastPos = newPos
  if (scrollTimer) {
    clearTimeout(scrollTimer)
  }
  scrollTimer = setTimeout(function() {
    scrollLastPos = null
  }, 30)
}

addEventListener("scroll", track, false)

// determine if a given element (plus an additional "bounds" area around it) is in the viewport
function viewable(el, bounds) {
  var rect = el.getBoundingClientRect()
  return (
    (rect.top + rect.height) >= 0 &&
    (rect.left + rect.width) >= 0 &&
    (rect.bottom - rect.height) <= ((window.innerHeight || document.documentElement.clientHeight) + bounds) &&
    (rect.right - rect.width) <= ((window.innerWidth || document.documentElement.clientWidth) + bounds)
  )
}

var initd = false, elements = [], elementsLength, reappear = [],
  appeared = 0, disappeared = 0, timer, deltaSet, opts = {}, done

// handle debouncing a function for better performance on scroll
function debounce(fn, delay) {
  return function() {
    var self = this, args = arguments
    clearTimeout(timer)

    timer = setTimeout(function() {
      fn.apply(self, args)
    }, delay)
  }
}

// called on scroll and resize event, so debounce the actual function that does
// the heavy work of determining if an item is viewable and then "appearing" it
function checkAppear() {
  if (scroll.delta < opts.delta.speed) {
    if (!deltaSet) {
      deltaSet = true
      doCheckAppear()
      setTimeout(function() {
        deltaSet = false
      }, opts.delta.timeout)
    }
  }
  (debounce(function() {
    doCheckAppear()
  }, opts.debounce)())
}

function begin() {
  // initial appear check before any scroll or resize event
  doCheckAppear()

  // add relevant listeners
  addEventListener("scroll", checkAppear, false)
  addEventListener("resize", checkAppear, false)
}

function end() {
  elements = []
  if (timer) {
    clearTimeout(timer)
  }
  removeListeners()
}

function removeListeners() {

  removeEventListener("scroll", checkAppear, false)
  removeEventListener("resize", checkAppear, false)
}

function doCheckAppear() {
  if (done) {
    return
  }

  elements.forEach(function(n, i) {
    if (n && viewable(n, opts.bounds)) {
      // only act if the element is eligible to reappear
      if (reappear[i]) {
        // mark this element as not eligible to appear
        reappear[i] = false
        // increment the count of appeared items
        appeared++

        // call the appear fn
        if (opts.appear) {
          opts.appear(n)
        }
        // if not tracking reappears or disappears, need to remove node here
        if (!opts.disappear && !opts.reappear) {
          // stop tracking this node, which is now viewable
          elements[i] = null
        }
      }
    }
    else {
      if (reappear[i] === false) {
        if (opts.disappear) {
          opts.disappear(n)
        }
        // increment the dissappeared count
        disappeared++

        // if not tracking reappears, need to remove node here
        if (!opts.reappear) {
          // stop tracking this node, which is now viewable
          elements[i] = null
        }
      }
      // element is out of view and eligible to be appeared again
      reappear[i] = true
    }
  })

  // remove listeners if all items have (re)appeared
  if (!opts.reappear && (!opts.appear || opts.appear && appeared === elementsLength) && (!opts.disappear || opts.disappear && disappeared === elementsLength)) {
    // ensure done is only called once (could be called from a trailing debounce/throttle)
    done = true
    removeListeners()
    // all items have appeared, so call the done fn
    if (opts.done) {
      opts.done()
    }
  }
}

function init() {
  // make sure we only init once
  if (initd) {
    return
  }
  initd = true

  // call the obj init fn
  if (opts.init) {
    opts.init()
  }
  // get the elements to work with
  var els
  if (typeof opts.elements === "function") {
    els = opts.elements()
  }
  else {
    els = opts.elements
  }
  if (els) {
    //  put elements into an array object to work with
    elementsLength = els.length
    for (var i = 0; i < elementsLength; i += 1) {
      elements.push(els[i])
      reappear.push(true)
    }
    begin()
  }
}

var appear = function(obj) {
  obj = obj || {}

  // assign the fn to execute when a node is visible
  opts = {
    // a function to be run when the dom is ready (allows for any setup work)
    init: obj.init,
    // either an array of elements or a function that will return an htmlCollection
    elements: obj.elements,
    // function to call when an element is "viewable", will be passed the element to work with
    appear: obj.appear,
    // function to call when an element is no longer "viewable", will be passed the element to work with
    disappear: obj.disappear,
    // function to call when all the elements have "appeared"
    done: obj.done,
    // keep tracking the elements
    reappear: obj.reappear,
    // the extra border around an element to make it viewable outside of the true viewport
    bounds: obj.bounds || 0,
    // the debounce timeout
    debounce: obj.debounce || 50,
    // appear.js will also check for items on continuous slow scrolling
    // you can controll how slow the scrolling should be  (deltaSpeed)
    // and when it will check again (deltaTimeout) after it has inspected the dom/viewport;
    delta: {
      speed: obj.deltaSpeed || 50,
      timeout: obj.deltaTimeout || 500,
    },
  }

  // add an event listener to init when dom is ready
  addEventListener("DOMContentLoaded", init, false)

  // http://stackoverflow.com/questions/9900311/how-do-i-target-only-internet-explorer-10-for-certain-situations-like-internet-e/13971998#13971998
  var isIE10 = false
  if (Function("/*@cc_on return document.documentMode===10@*/")()) {
    isIE10 = true
  }
  var completeOrLoaded = document.readyState === "complete" || document.readyState === "loaded"

  // call init if document is ready to be worked with and we missed the event
  if (isIE10) {
    if (completeOrLoaded) {
      init()
    }
  }
  else {
    if (completeOrLoaded || document.readyState === "interactive") {
      init()
    }
  }

  return {
    // manually fire check for visibility of tracked elements
    trigger: function trigger() {
      doCheckAppear()
    },
    // pause tracking of elements
    pause: function pause() {
      removeListeners()
    },
    // resume tracking of elements after a pause
    resume: function resume() {
      begin()
    },
    // provide a means to stop monitoring all elements
    destroy: function destroy() {
      end()
    },
  }

}

module.exports = appear

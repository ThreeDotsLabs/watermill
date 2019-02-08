// Intialize layout
var container = document.getElementById("container");
var content = document.getElementById("content");
var clientWidth = 0;
var clientHeight = 0;

// Initialize Scroller
this.scroller = new Scroller(render, {
	zooming: true
});


var scrollLeftField = document.getElementById("scrollLeft");
var scrollTopField = document.getElementById("scrollTop");
var zoomLevelField = document.getElementById("zoomLevel");

setInterval(function() {
	var values = scroller.getValues();
	scrollLeftField.value = values.left.toFixed(2);
	scrollTopField.value = values.top.toFixed(2);
	zoomLevelField.value = values.zoom.toFixed(2);
}, 500);


var rect = container.getBoundingClientRect();
scroller.setPosition(rect.left + container.clientLeft, rect.top + container.clientTop);


// Reflow handling
var reflow = function() {
	clientWidth = container.clientWidth;
	clientHeight = container.clientHeight;
	scroller.setDimensions(clientWidth, clientHeight, contentWidth, contentHeight);
};

window.addEventListener("resize", reflow, false);
reflow();

var checkboxes = document.querySelectorAll("#settings input[type=checkbox]");
for (var i=0, l=checkboxes.length; i<l; i++) {
	checkboxes[i].addEventListener("change", function() {
		scroller.options[this.id] = this.checked;
	}, false);
}

document.querySelector("#settings #zoom").addEventListener("click", function() {
	scroller.zoomTo(parseFloat(document.getElementById("zoomLevel").value));
}, false);

document.querySelector("#settings #zoomIn").addEventListener("click", function() {
	scroller.zoomBy(1.2, true);
}, false);

document.querySelector("#settings #zoomOut").addEventListener("click", function() {
	scroller.zoomBy(0.8, true);
}, false);

document.querySelector("#settings #scrollTo").addEventListener("click", function() {
	scroller.scrollTo(parseFloat(document.getElementById("scrollLeft").value), parseFloat(document.getElementById("scrollTop").value), true);
}, false);

document.querySelector("#settings #scrollByUp").addEventListener("click", function() {
	scroller.scrollBy(0, -150, true);
}, false);

document.querySelector("#settings #scrollByRight").addEventListener("click", function() {
	scroller.scrollBy(150, 0, true);
}, false);

document.querySelector("#settings #scrollByDown").addEventListener("click", function() {
	scroller.scrollBy(0, 150, true);
}, false);

document.querySelector("#settings #scrollByLeft").addEventListener("click", function() {
	scroller.scrollBy(-150, 0, true);
}, false);


if ('ontouchstart' in window) {

	container.addEventListener("touchstart", function(e) {
		// Don't react if initial down happens on a form element
		if (e.touches[0] && e.touches[0].target && e.touches[0].target.tagName.match(/input|textarea|select/i)) {
			return;
		}

		scroller.doTouchStart(e.touches, e.timeStamp);
		e.preventDefault();
	}, false);

	document.addEventListener("touchmove", function(e) {
		scroller.doTouchMove(e.touches, e.timeStamp, e.scale);
	}, false);

	document.addEventListener("touchend", function(e) {
		scroller.doTouchEnd(e.timeStamp);
	}, false);

	document.addEventListener("touchcancel", function(e) {
		scroller.doTouchEnd(e.timeStamp);
	}, false);

} else {

	var mousedown = false;

	container.addEventListener("mousedown", function(e) {
		if (e.target.tagName.match(/input|textarea|select/i)) {
			return;
		}
		
		scroller.doTouchStart([{
			pageX: e.pageX,
			pageY: e.pageY
		}], e.timeStamp);

		mousedown = true;
	}, false);

	document.addEventListener("mousemove", function(e) {
		if (!mousedown) {
			return;
		}
		
		scroller.doTouchMove([{
			pageX: e.pageX,
			pageY: e.pageY
		}], e.timeStamp);

		mousedown = true;
	}, false);

	document.addEventListener("mouseup", function(e) {
		if (!mousedown) {
			return;
		}
		
		scroller.doTouchEnd(e.timeStamp);

		mousedown = false;
	}, false);

	container.addEventListener(navigator.userAgent.indexOf("Firefox") > -1 ? "DOMMouseScroll" :  "mousewheel", function(e) {
		scroller.doMouseZoom(e.detail ? (e.detail * -120) : e.wheelDelta, e.timeStamp, e.pageX, e.pageY);
	}, false);

}

/*
// Test for background activity (slow down scrolling)
setInterval(function() {
	var arr = [];
	for (var i=0, l=Math.random()*600; i<l; i++) {
		arr.push.call(arr, document.querySelectorAll(".abc" + i));
	}
}, 50);
*/

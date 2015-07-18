//-------------------------------------------------------------------
//A number of forward declarations. These variables need to be defined since 
//they are attached to static code in HTML. But we cannot define them yet
//since they need D3.js stuff. So we put placeholders.


//Highlight a node in the graph. It is a closure within the d3.json() call.
var selectNode = undefined;

//Change status of a panel from visible to hidden or vice versa
var toggleDiv = undefined;

//The call to set a zoom value -- currently unused
//(zoom is set via standard mouse-based zooming)
var zoomCall = undefined;


//-------------------------------------------------------------------

// plot a graph -- to be called after D3.js has loaded
function plot_graph(evt) {

    var options = {
        width:              960,
        height:             600,
        directed:           true,
        label_always_on:    true,
        label_on_threshold: 2.5,
        uri:                "/link/"
    };
    
    function merge(obj1, obj2) {
        for (var attrname in obj2) {
            obj1[attrname] = obj2[attrname];
        }
    }
    
    merge(options, evt.target.options);

    // Variables keeping graph state
    var activeNode = undefined;
    var currentOffset = { x : 0, y : 0 };
    var currentZoom = 1.0;

    // The D3.js scales
    var xScale = d3.scale.linear()
        .domain([0, options.width])
        .range([0, options.width]);
    var yScale = d3.scale.linear()
        .domain([0, options.height])
        .range([0, options.height]);
    var zoomScale = d3.scale.linear()
        .domain([1,6])
        .range([1,6])
        .clamp(true);

    /* .......................................................................... */

    // The D3.js force-directed layout
    var force = d3.layout.force()
        .charge(-320)
        .size( [options.width, options.height] )
        .linkStrength( function(d,idx) { return d.weight; } );

    // Add to the page the SVG element that will contain the node network
    var svg = d3.select("#network").append("svg:svg")
        .attr('xmlns','http://www.w3.org/2000/svg')
        .attr("width", options.width)
        .attr("height", options.height)
        .attr("id","graph")
        .attr("viewBox", "0 0 " + options.width + " " + options.height )
        .attr("preserveAspectRatio", "xMidYMid meet");

    /* ....................................................................... */

    // Get the current size & offset of the browser's viewport window
    function getViewportSize( w ) {
        var w = w || window;
        if( w.innerWidth != null ) 
            return {
                w: w.innerWidth, 
                h: w.innerHeight,
                x : w.pageXOffset,
                y : w.pageYOffset
            };
            
        var d = w.document;
        if( document.compatMode == "CSS1Compat" )
            return {
                w: d.documentElement.clientWidth,
                h: d.documentElement.clientHeight,
                x: d.documentElement.scrollLeft,
                y: d.documentElement.scrollTop
            };
        else
            return {
                w: d.body.clientWidth, 
                h: d.body.clientHeight,
                x: d.body.scrollLeft,
                y: d.body.scrollTop
            };
    }

    /* Change status of a panel from visible to hidden or vice versa
     id: identifier of the div to change
     status: 'on' or 'off'. If not specified, the panel will toggle status
     */
    toggleDiv = function( id, status ) {
        d = d3.select('div#'+id);
        if( status === undefined )
            status = d.attr('class') == 'panel_on' ? 'off' : 'on';
        d.attr( 'class', 'panel_' + status );
        return false;
    }

    // *************************************************************************
    d3.xhr(options.uri + evt.target.node, 'application/json', function(error, data) {
        if (error !== null) return;

        var graphData = JSON.parse(data.response);
        // Declare the variables pointing to the node & link arrays
        var nodeArray = graphData.nodes;
        var linkArray = graphData.links;
        startGraph(nodeArray, linkArray);

        var graphLinks;
        var graphNodes;
        var graphLabels; 

        /* Initialize graph */
        function startGraph(nodeArray, linkArray) {
            var minNodeWeight = 
                Math.min.apply( null, nodeArray.map( function(n) {return n.rank;} ) );
            var maxNodeWeight = 
                Math.max.apply( null, nodeArray.map( function(n) {return n.rank;} ) );

            var minLinkWeight = 
                Math.min.apply( null, linkArray.map( function(n) {return n.weight;} ) );
            var maxLinkWeight = 
                Math.max.apply( null, linkArray.map( function(n) {return n.weight;} ) );

            // Add the node & link arrays to the layout, and start it
            force.nodes(nodeArray).links(linkArray).start();

            // A couple of scales for node radius & edge width
            var nodeSize = d3.scale.linear()
                .domain([minNodeWeight,maxNodeWeight])
                .range([5,12])
                .clamp(true);
            var edgeWidth = d3.scale.pow().exponent(8)
                .domain([minLinkWeight,maxLinkWeight])
                .range([1,3])
                .clamp(true);

            /* Add drag & zoom behaviors */
            svg.call(d3.behavior.drag()
                    .on("drag",dragMove));
            svg.call(d3.behavior.zoom()
                    .x(xScale)
                    .y(yScale)
                    .scaleExtent([1, 6])
                    .on("zoom", doZoom));

            // ------- Create the elements of the layout (links and nodes) ------
            // clear up SVG
            d3.select(".grpParent").remove();      
            var networkGraph = svg.append('svg:g').attr('class','grpParent');

            var diagonal = d3.svg.diagonal().projection(function(d) { return [d.y, d.x]; });
            svg.append("defs").append("marker")
                .attr("id", "arrowhead")
                .attr("refX", 6 + 3) /*must be smarter way to calculate shift*/
                .attr("refY", 2)
                .attr("markerWidth", 6)
                .attr("markerHeight", 4)
                .attr("orient", "auto")
                .append("path")
                .attr("d", "M 0,0 V 4 L6,2 Z");

            // links: simple lines
            graphLinks = networkGraph.append('svg:g').attr('class','grp gLinks')
                .selectAll("line")
                .data(linkArray)
                .enter().append("line")
                .style('stroke-width', function(d) { return edgeWidth(d.weight);} )
                .attr("class", "link")
                .attr("marker-end", "url(#arrowhead)")
                .attr("d", diagonal);

            // nodes: an SVG circle
            graphNodes = networkGraph.append('svg:g').attr('class','grp gNodes')
                .selectAll("circle")
                .data( nodeArray, function(d){return d.id} )
                .enter().append("svg:circle")
                //.attr('r', 5)
                .attr('r', function(d) { return nodeSize(d.rank);} )
                .attr('id', function(d) { return "c" + d.index; } )
                .attr('pointer-events', 'all')  
                .on("click", function(d) { addLinks(d); } );

            // labels: a group with two SVG text: a title and a shadow (as background)
            graphLabels = networkGraph.append('svg:g').attr('class','grp gLabel')
                .selectAll("g.label")
                .data( nodeArray, function(d){return d.id} )
                .enter().append("svg:g")
                .attr('id', function(d) { return "l" + d.index; } )
                .attr('class','label');

            var shadows = graphLabels.append('svg:text')
                .attr('x','-2em')
                .attr('y','-.3em')
                .attr('pointer-events', 'none') // they go to the circle beneath
                .attr('id', function(d) { return "lb" + d.index; } )
                .attr('class','nshadow')
                .text( function(d) { return d.id; } );

            var labels = graphLabels.append('svg:text')
                .attr('x','-2em')
                .attr('y','-.3em')
                .attr('pointer-events', 'none') // they go to the circle beneath
                .attr('id', function(d) { return "lf" + d.index; } )
                .attr('class','nlabel')
                .text( function(d) { return d.id; } );      
        }

        /* --------------------------------------------------------------------- */
        /* Select/unselect a node in the network graph.
       Parameters are: 
       - node: data for the node to be changed,  
       - on: true/false to show/hide the node
         */
        function highlightGraphNode( node, on )
        {
            if (typeof node == 'undefined') return;

            // If we are to activate a node, and there's already one active,
            // first switch that one off
            if( on && activeNode !== undefined ) {
                //highlightGraphNode( activeNode, false );
            }

            // locate the SVG nodes: circle & label group
            circle = d3.select( '#c' + node.index );
            label  = d3.select( '#l' + node.index );

            // activate/deactivate the node itself
            circle.classed( 'main', on );
            label.classed( 'on', on || currentZoom >= options.label_on_threshold );
            label.selectAll('text').classed( 'main', on );

            // set the value for the current active node
            activeNode = on ? node : undefined;
        }


        /* --------------------------------------------------------------------- */
        /* Highlight the node in the graph. Also called from outside the d3.json context.
       Parameters:
       - new_idx: index of the node to show
       - doMoveTo: boolean to indicate if the graph should be centered
         on the node
         */
        selectNode = function( new_idx, doMoveTo ) {

            // do we want to center the graph on the node?
            doMoveTo = doMoveTo || false;
            if( doMoveTo ) {
                var s = getViewportSize();
                var width  = s.w < options.width  ? s.w : options.width;
                var height = s.h < options.height ? s.h : options.height;

                var offset = {
                        x : s.x + width/2 - nodeArray[new_idx].x*currentZoom,
                        y : s.y + height/2 - nodeArray[new_idx].y*currentZoom };
                repositionGraph( offset, undefined, 'move' );
            }
            // Now highlight the graph node
            highlightGraphNode( nodeArray[new_idx], true );
        }

        /* add links from a given node
         */
        function addLinks( node ) {
            d3.xhr(options.uri + node.id, 'application/json', function(error, data) {
                if (error !== null) return;

                var additionalData = JSON.parse(data.response);
                var newNodeArray = additionalData.nodes;
                var newLinkArray = additionalData.links;

                var numOldNodes = nodeArray.length;
                for (var i in newNodeArray) {
                    var newNode = newNodeArray[i];
                    var idx = -1;
                    for (var j in nodeArray) {
                        if (j >= numOldNodes) break;
                        var oldNode = nodeArray[j];
                        if (oldNode.id === newNode.id) {
                            idx = oldNode.index;
                            break;
                        }
                    }

                    var nodeIdx = newNode.index;
                    if (idx === -1) {
                        idx = nodeArray.length;
                        newNode.index = idx;
                        nodeArray.push(newNode);
                    }

                    for (var j in newLinkArray) {
                        var link = newLinkArray[j];
                        if (link.source === nodeIdx) {
                            link.source = -idx;
                        }
                        if (link.target === nodeIdx) {
                            link.target = -idx;
                        }
                    }
                }

                for (var i in newLinkArray) {
                    var link = newLinkArray[i];
                    if (link.source < 0) {
                        link.source = -link.source;
                    }
                    if (link.target < 0) {
                        link.target = -link.target;
                    }
                }

                for (var i in linkArray) {
                    var link = linkArray[i];
                    link.source = link.source.index;
                    link.target = link.target.index;
                }

                linkArray = linkArray.concat(newLinkArray);

                startGraph(nodeArray, linkArray);
            });
        }

        /* Move all graph elements to its new positions. Triggered:
       - on node repositioning (as result of a force-directed iteration)
       - on translations (user is panning)
       - on zoom changes (user is zooming)
       - on explicit node highlight (user clicks in a node panel link)
       Set also the values keeping track of current offset & zoom values
         */
        function repositionGraph( off, z, mode ) {

            // do we want to do a transition?
            var doTr = (mode == 'move');

            // drag: translate to new offset
            if( off !== undefined && (off.x != currentOffset.x || off.y != currentOffset.y ) ) {
                g = d3.select('g.grpParent')
                if( doTr ) g = g.transition().duration(500);
                g.attr("transform", function(d) { return "translate("+off.x+","+off.y+")" } );
                currentOffset.x = off.x;
                currentOffset.y = off.y;
            }

            // zoom: get new value of zoom
            if( z === undefined ) {
                if( mode != 'tick' ) return;    // no zoom, no tick, we don't need to go further
                z = currentZoom;
            }
            else
                currentZoom = z;

            // move edges
            e = doTr ? graphLinks.transition().duration(500) : graphLinks;
            e.attr("x1", function(d) { return z*(d.source.x); })
             .attr("y1", function(d) { return z*(d.source.y); })
             .attr("x2", function(d) { return z*(d.target.x); })
             .attr("y2", function(d) { return z*(d.target.y); });

            // move nodes
            n = doTr ? graphNodes.transition().duration(500) : graphNodes;
            n.attr("transform", function(d) {
                return "translate("+z*d.x+","+z*d.y+")"
            } );
            // move labels
            l = doTr ? graphLabels.transition().duration(500) : graphLabels;
            l.attr("transform", function(d) { return "translate("+z*d.x+","+z*d.y+")" } );
        }


        /* --------------------------------------------------------------------- */
        /* Perform drag
         */
        function dragMove(d) {
            offset = {
                x : currentOffset.x + d3.event.dx,
                y : currentOffset.y + d3.event.dy
            };
            repositionGraph( offset, undefined, 'drag' );
        }


        /* --------------------------------------------------------------------- */
        /* Perform zoom. We do "semantic zoom", not geometric zoom
         * (i.e. nodes do not change size, but get spread out or stretched
         * together as zoom changes)
         */
        function doZoom( increment ) {
            newZoom = increment === undefined ? d3.event.scale 
                    : zoomScale(currentZoom+increment);
            if( currentZoom == newZoom ) return;    // no zoom change

            // See if we cross the 'show' threshold in either direction
            if( currentZoom<options.label_on_threshold && newZoom>=options.label_on_threshold )
                svg.selectAll("g.label").classed('on',true);
            else if( currentZoom>=options.label_on_threshold && newZoom<options.label_on_threshold )
                svg.selectAll("g.label").classed('on',false);

            // See what is the current graph window size
            s = getViewportSize();
            width  = s.w < options.width  ? s.w : options.width;
            height = s.h < options.height ? s.h : options.height;

            // Compute the new offset, so that the graph center does not move
            zoomRatio = newZoom/currentZoom;
            newOffset = {
                x : currentOffset.x*zoomRatio + width/2*(1-zoomRatio),
                y : currentOffset.y*zoomRatio + height/2*(1-zoomRatio)
            };

            // Reposition the graph
            repositionGraph( newOffset, newZoom, "zoom" );
        }

        zoomCall = doZoom;    // unused, so far

        /* --------------------------------------------------------------------- */

        /* process events from the force-directed graph */
        force.on("tick", function() {
            repositionGraph(undefined,undefined,'tick');
        });
    });

} // end of plot_graph()

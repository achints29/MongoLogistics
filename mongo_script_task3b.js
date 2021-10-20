minsize = {$match:{population:{$gt:1000}}}
sortbysize = { $sort : { population: -1 }}
groupByCountry = { $group : { _id: "$country", allCities : { $addToSet : "$$ROOT" }}}
slicedTo15orLess = { $project : { slicedTo15orLess: {$slice:["$allCities",0,15]} }}
unwind = {$unwind:{path:"$slicedTo15orLess",includeArrayIndex:'false'}}
format = { $project : { _id: { $replaceAll: { input: { $concat: [ "$slicedTo15orLess.city_ascii", "_", "$slicedTo15orLess.iso2" ] }, find: '/', replacement: '_' } }, position:["$slicedTo15orLess.lng","$slicedTo15orLess.lat"] , country: "$slicedTo15orLess.country" }}
newcollection = { $out : "cities" }
db.worldcities.aggregate([minsize,sortbysize,groupByCountry,slicedTo15orLess,unwind,format,newcollection])

firstN = { $sample: { size: 200} }
addidone = { $group: { _id: null, planes : { $push : { currentLocation :"$position" }}}}
unwind = { $unwind : {path: "$planes", includeArrayIndex: "id" }}
format = {$project : { _id : {$concat : ["CARGO",{$toString:"$id"}]},
currentLocation: "$planes.currentLocation", heading:{$literal:0}, route: []}}
asplanes = { $out: "planes"}
db.cities.aggregate([firstN,addidone,unwind,format,asplanes])
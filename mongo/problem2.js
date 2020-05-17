use ds503
db.getCollection("famous-people").find()
db.getCollection("famous-people").mapReduce(
    function(){
        for(var i in this.awards){
            emit(this.awards[i].award, 1)
        }
    },
    function(key, values){
        return Array.sum(values)
    },
    
    {out: "count"}
)

db.count.find()

db.getCollection("famous-people")
.aggregate([
    {$unwind: "$awards"},
    {$group: {_id: "$awards.award", years: {"$addToSet" : "$awards.year"}}}
])

db.getCollection("famous-people")
.aggregate([
    {$group: {_id: {$year: "$birth"}, count: {$sum: 1}, idArray: {"$addToSet": "$_id"}}}
])

var ids = []
db.getCollection("famous-people")
.aggregate([
    {$group: {_id: 1, minId: {$min: "$_id"}, maxId: {$max: "$_id"}}}
]).map(function(record){
    ids.push(record.minId),
    ids.push(record.maxId)
})
db.getCollection("famous-people").find(
    {_id: {$in: ids}}
)

db.getCollection("famous-people")
.createIndex({"awards.award":"text"})
db.getCollection("famous-people")
.find({ $text:{$search: "Turing"}})

db.getCollection("famous-people")
.find({ $text: {$search: "Turing, National Medal"}})




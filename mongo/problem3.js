db.createCollection("problem3")

db.problem3.insert({ _id: "MongoDB", parent: "Databases" })
db.problem3.insert({ _id: "dbm", parent: "Databases" })
db.problem3.insert({ _id: "Databases", parent: "Programming" })
db.problem3.insert({ _id: "Languages", parent: "Programming" })
db.problem3.insert({ _id: "Programming", parent: "Books" })
db.problem3.insert({ _id: "Books", parent: null })

var node = db.problem3.findOne({"_id": "MongoDB"})
var level = 1
var res = []
while(node.parent){
    var node = db.problem3.findOne({"_id": node.parent})
    res.push({
        "Name": node._id,
        "Level": level
    })
    level += 1
}

res

var height = 0
var root = db.problem3.findOne({"_id" : "Books"})
var queue = []
queue.push(root)
var size = queue.length

while(queue.length > 0){ 
   for(let i = 0; i < size; i++){
       var cur = queue.shift();
       var nxt = db.problem3.find({parent: cur._id});
       while(nxt.hasNext() == true){
           var tmp = nxt.next();
           queue.push(tmp);
       }
   }
   size = queue.length;
   height += 1;
}

height


db.createCollection("problem3CR")
db.problem3CR.insert( {_id: "MongoDB", children: []} )
db.problem3CR.insert( {_id: "dbm", children: []} )
db.problem3CR.insert( {_id: "Databases", children: ["MongoDB", "dbm" ]} )
db.problem3CR.insert( {_id: "Languages", children: []} )
db.problem3CR.insert( {_id: "Programming", children: ["Databases", "Languages"]} )
db.problem3CR.insert( {_id: "Books", children: ["Programming"]} )


db.problem3CR.find({"children": "dbm"})

var queue = []
var res = []
var root = db.problem3CR.findOne({ "_id": "Books" })
queue.push(root)

while(queue.length > 0){
    var cur = queue.shift()
    
    for(let i = 0; i < cur.children.length; i++){
        var tmp = db.problem3CR.findOne({"_id": cur.children[i]})
        queue.push(tmp)
        res.push(tmp._id)
    }
}

res

var parent = db.problem3CR.findOne({"children": "Databases"})
var res = []
for(let i = 0; i < parent.children.length; i++){
    var tmp = parent.children[i]
    if(tmp != "Databases"){
        res.push(db.problem3CR.findOne({"_id": tmp}))
    }
}

res



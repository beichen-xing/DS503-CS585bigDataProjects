// 1) Write an operation(s) that changes the _id of “John McCarthy” to value 100.

	 //for this we  cannot update it directly. We have to save the document using a new _id, and then remove the old document.

	 //Saving John's McCarthy document to a new variable.

	  john_doc = db.bios.findOne({
	  	_id: ObjectId("51df07b094c6acd67e492f41")
	  });

	  //changing the old value to 100

	  john_doc._id = 100

	  //inserting the new document to the collection
	  db.bios.insert(john_doc)

	  //removing the old one

	  db.bios.remove({_id: ObjectId("51df07b094c6acd67e492f41")})


//2) Write an operation(s) that inserts the following new records into the collection:
	db.bios.insertMany([
	{
		
		"_id" : 20, 

		"name" : {
			"first" : "Mary",
			"last" : "Sally" 
		}, 
		"birth" : ISODate("1933-08-27T04:00:00Z"), 
		"death" : ISODate("1984-11-07T04:00:00Z"), 
		"contribs" : [
		"C++",
		"Simula" 
		], 
		"awards" : [
		{
			"award" : "WPI Award",
			"year" : 1999,
			"by" : "WPI"

		}
		]
		
	},
	{
		"_id" : 30, 
		"name" : { 
			"first" : "Ming",
			"last" : "Zhang" 
		}, 
		"birth" : ISODate("1911-04-12T04:00:00Z"), 
		"death" : ISODate("2000-11-07T04:00:00Z"), 
		"contribs" : [ 
			"C++", 
			"FP", 
			"Python", 
		], 
		"awards" : [ 
			{ 
				"award" : "WPI Award", 
				"year" : 1960, 
				"by" : "WPI" 
			},
			{ 
			"award" : "Turing Award", 
			"year" : 1960, 
			"by" : "ACM" 
		}
		]

	}
]);


//3) Report all documents of people who got a “Turing Award” after 1940.

db.bios.find(
	{
		awards: {
        	$elemMatch: {
            	award: "Turing Award",
            	year: { $gt: 1940 }
        	}
      	}
	}
);


//4) Report all people who got more than 1 award.

db.bios.find(
	{
		
	 $where: "this.awards ? this.awards.length < 3 : false" 

	}	
);

// 5) Update the document of “Guido van Rossum” to add “Python” to the contribution list.

db.test.update(
	{
        name: {
            "first": "Guido",
            "last": "van Rossum"
        }
	},
   	{
		$push: { contribs: "Python" }
   }
);

//6) Insert a new field called “comments” of type array into document of “Mary Sally” 
//storing the comments: “taught in 3 universities”, “was an amazing pioneer”, “lived in Worcester.”

db.bios.update(
	{
        name: {
            "first": "Mary",
            "last": "Sally"
        }
	},
   	{
		$set: {
			comments: [
				"taught in 3 universities",
				"was an amazing pioneer",
				"lived in Worcester."
			]
		}
   }
);

/* 7) For each contribution by “Mary Sally”, say contribution “C”, list the people’s first and last names 
who have the same contribution “C”. For example, since “Mary Sally” has two contributions in 
“C++” and “Simula”, the output for her should be similar to:

{Contribution: “C++”, 
People: [{first: “Mary”, last: “Sally”}, {first: “Ming”, last: “Zhang”}]}, 
{ Contribution: “Simula”, … .} */

var contributions=[];

db.bios.find(
	{
        name: {
            "first": "Mary",
            "last": "Sally"
        }
    }
).forEach(
	function(u){
		contributions=u.contribs
	}
	);
db.bios.aggregate(
        [
        	{$unwind:"$contribs"},
		{$match:{'contribs':{$in: contributions}}},
		{$group:{_id: "$contribs",people:{$push:"$name"}}}
            ]
);

// 8) Report all documents where the first name matches the regular expression “Jo*”, 
// where “*” means any number of characters. Report the documents sorted by the last name.

db.bios.find(
	{
        "name.first": { $regex: /^Jo/ }
    },
    {
    	_id: 0,
    	name: 1
    }
).sort({ 'name.last': 1 });

// 9) Update the award of document _id =30, which is given by WPI, and set the year to 1965.

db.bios.update(
   	{
   		_id: 30,
   		"awards.by": "WPI"
   	},
   	{
   		$set: {
   			"awards.$.year": 1965
   		}
   	}
);

//10) Add (copy) all the contributions of document _id = 3 to that of document _id = 30.
doc = db.bios.findOne(
	{
        _id: 3
    },
    {
    	_id: 0,
    	contribs: 1
    }
);
contribs = doc.contribs;

db.bios.update(
	{
        _id: 30
	},
   	{
		$push: { contribs: { $each: contribs } }
   }
);















































	



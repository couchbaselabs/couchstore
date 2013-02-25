# The Binary (Termless) Format for Views. #

**(Damien Katz -- July 19 2012)**

This documents the format of the Key/Value pairs in leaf nodes, and the reduction format in inner nodes.

The underlying b-tree format is the same as already used in CouchStore.

* All integers are network byte order.
* All strings are encoded as UTF-8.
* Adjacent fields are tightly packed with no padding.


## Primary Index Key Values ##

In leaf nodes, `KeyValues` have the following format:

* `Key`:
  *  `EmittedJsonKeyLength` -- 16bit integer
  *  `EmittedJSONKey` -– JSON -– The key emitted by the map function
  *  `UnquotedDocId` –- String -– The raw doc ID (occupies the remaining bytes)
* Value:
  *  `PartitionId` -- 16bit integer -- This is the partitionId (vbucket) from which this document id maps to.
  *  1 to infinity `JSONStringValue`s -- These are all the values that were emitted for this `EmittedJSONKey`.  
     Each `JSONStringValue` is of the form:
		* `ValueLength` -- 24bit unsigned integer
		* `JSONValue` - string that is `ValueLength` bytes long

(Parsing the `JSONStringValue`s is simply reading the first 24 bits, getting the length of the following string and extracting the string. If there is still buffer left, the process is repeated until the is no value buffer left.)

When an emit happens, and the Key is different from all other keys emitted for that document, then there is only one `JSONStringValue`.
But when multiple identical keys are emitted, the values are coalesced into a list of Values, and there will be multiple values.


### Primary Index Inner Node Reductions (KeyPointerNodes and Root) ###

* `SubTreeCount` -- 40bit integer -- Count of all Values in subtree.  
	NOTE: this is possibly greater than the `KeyId` count in the subtree, because a document can emit multiple identical keys, and they are coalesced into single `KeyId`, with all the values emitted in a list as the value.
* `SubTreePartitionBitmap` -- 1024 bits -- a bitfield of all partition keys in the subtree. Currently this is hardcoded at 1024 bits in length, but in the future we may change this to a variable size. Until then, it works with any # of vbuckets ≤ 1024.
* `JSONReductions` -- remaining bytes -- Zero or more `JSONReductions`, each consisting of:
  *  `JSONLen` -- 16bit integer
  *  `JSON` -- the actual JSON string


## Back Index ##

In leaf nodes, `KeyValues` have the following format:

* `Key` -- blob -- The raw docId, not quoted or JSONified in any way.
* `Value`:
  * `PartitionId` -- 16bit integer -- This is the partitionId (vbucket) from which this document id maps to.
  * 1-*n* `ViewKeysMappings`, where *n* ≤ the # of mapfunctions defined in the design document.  
    A `ViewKeysMapping` is:
	    * `ViewId` -- 8bit integer -- the ordinal id of the map view in the group the following keys were emitted from
	    * `NumKeys` -- 16bit integer -- the number of `JSONKeys` that follow
		* `JSONKeys` -- a sequence of:
			  * `KeyLen` -- 16bit integer -- Length of the following `JSONKey`
		  	  * `Key` -- JSON string -- Emitted JSON key


### Back Index Inner Node Reductions (KeyPointerNodes and Root) ###

* `SubTreeCount` -- 40bit integer -- count of all Keys in subtree.
* `SubTreePartitionBitmap` -- 1024 bits -- a bitfield of all partition keys in the subtree. Currently this is hardcoded at 1024 in length, but in the future we may change this to a variable size. Until then, it works with any # of vbuckets ≤ 1024.

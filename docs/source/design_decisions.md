# Design decisions

## Code structure
TODO

### Relations
When denoting a relation as ClassA-ClassB, the first class is always the controlling class, 
and the second one is therefore always the controlled class. 

* Internal cells and their parent cell: ICell-ICompositeCell
* Parent cell observing its internal cells: IObserver-IObservable
* IO and their cell: IO-ICell
* Clone cells and their original cell: IObserver-IObservable
* Connections and their parent cell: IConnection-ICompositeCell
* Connections and their source: IConnection-IConnectionExitPoint
* Connections and their target: IConnection-IConnectionEntryPoint
* ...

## Conventions

### Naming conventions
* Names of interfaces start with an "I" (ex: `ICell`). Classes can inherit any number of interfaces.
* Names of abstract classes start with an "A" (ex: `ACell`). Classes should inherit at most 1 (abstract) class that is 
  not an interface. 

### Validation
To validate our system, the relations of all objects and their relations must be validated. They must be valid at all 
times, and if they aren't, this means there is a bug in the code. In other words, by only using the public methods 
available in our system, we should never end up in an invalid state. 
So in theory, when all tests of this codebase succeed and all possible configuration cases are covered without ever 
getting into an invalid state, these validation checks are not necessary anymore and can be ignored. However, during 
testing they prove very valuable. 

This validation happens in multiple steps of granularity. From finest to coarsest granularity, we define the 
following validators for every object: 

1. `can_have_as_X(value: X) -> BoolExplained`: validating an attribute X. Required for every attribute/relation 
   of the object. Ex: can a connection have a given `Output` object as source?
2. `can_have_as_nb_X(value: int) -> BoolExplained`: validating the number of attributes X. Optional, only needed when 
   specific limits on the relation with X are defined. Ex: can an `Output` object have more than 6 outgoing connections?
3. `assert_has_proper_X() -> None`: validating an attribute X and the relation with it. It usually combines 
   the `can_have_as_X` validator (and the optional `can_have_as_nb_X` validator) with other relational checks like 
   mutual reference consistency. Required for every attribute/relation of the object. It encapsulates the class 
   invariant for the relation with X, therefore it should never be invalid (in steady-state). Raise san 
   InvalidStateException when X is invalid. Ex: is the relation of a connection with its source valid? 
4. `assert_is_valid() -> None`: validating all attributes/relations of the object. Combines all 
   the `assert_has_proper_X` validators of all attributes/relations of the object. It encapsulates all class 
   invariants of the object, therefore it should never be invalid (in steady-state). Raises an InvalidStateException if 
   invalid. Only one such validator is required per object. 

In PyPipeline, the following methods are added to a cell to check whether the cell is deployable. 
In contrast to the previous validators, this check should not necessarily be true all the time. 
It is possible to define a system which is perfectly valid, but which is not deployable: for example a cell that has an 
input without any incoming connection to it. 

5. `is_deployable() -> BoolExplained`: checks whether all inputs of a cell are provided and whether the cell is 
   internally ready to be pulled as well.

explain select 1 is distinct from 1,
               1 is distinct from 2,
               1 is distinct from null,
               null is distinct from null
         from part;

select 1 is distinct from 1,
               1 is distinct from 2,
               1 is distinct from null,
               null is distinct from null
         from part;

explain select 1 is not distinct from 1,
               1 is not distinct from 2,
               1 is not distinct from null,
               null is not distinct from null
         from part;

select 1 is not distinct from 1,
               1 is not distinct from 2,
               1 is not distinct from null,
               null is not distinct from null
         from part;

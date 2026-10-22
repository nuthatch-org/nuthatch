# The indexed address parameters of the tables in $tabs, one UNION branch per table.
# $mode "role" keeps the parameter's name (as `role`); "plain" gives the address alone, which is
# shorter, since /sql refuses a statement over 16 KiB.
# Parameters named in .identifiers (an allocation id) are left out: they are new on every row.
(.identifiers // []) as $id
| .roles
| map(select((.table as $t | $tabs | index($t)) and (.col as $c | $id | index($c) | not)))
| group_by(.table)
| map("SELECT "
      + (if $mode == "role" then
           "unnest(make_array(" + (map("named_struct('r', '\(.col)', 'a', \"\(.col)\")") | join(", ")) + ")) AS u"
         else
           "unnest(make_array(" + (map("\"\(.col)\"") | join(", ")) + ")) AS a"
         end)
      + ", block_number\($extra) FROM \"\(.[0].table)\" WHERE block_number BETWEEN \($lo) AND \($hi)")
| join(" UNION ALL ")

gkron
=====

cron-like http task distributors 



                |------worker
                |
                |
        master--|------worker
          |     |
          |     |
          |     |------worker
          |
     task storage
        /   \
       /     \
     redis   db(not yet implemented)

once task: execute just once like javascript settimeout
    
interval task: execute repeated in certain interval like javascript setinterval

cron task: cron style time schedule(weekday not supported yet)

main.py contain examples you need

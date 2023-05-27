Seek idzie od najwyzszych poziomów ([MemTable](/db/memtable.cc)) do najniższych (poziomy od 0 do 10),
W MemTable używa bezpośredniego znalezienia (iterator ma w funkcji [Seek](/db/skiplist.h) funkcje findGreaterOrEqual)
Na poziomach([ForEachOverlapping](db/version_set.cc)) od 0 idzie po poszczegolnych plikach
i wyszukuje binarnie [Seek:164](table/block.cc) oraz uzywa filtra dostepnego w danym Table

Tworzenie Table [519](db/db_impl.cc)

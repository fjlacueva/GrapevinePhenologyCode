# import fsspec
# # of = fsspec.open("https://argon-docker.itainnova.es/repository/war/paraphrasing/participles.pickle", mode='rt', 
# #                  cache_storage='/tmp/participles.pickle', target_options={'anon': True})
# of = fsspec.open("http://devex.itainnova.es/maturolife/sw.js", mode='rt', 
#                  cache_storage='/tmp/participles.pickle', target_options={'anon': True})
# with of as f:
#     print(f.readline())
import fsspec
of = fsspec.open("http://devex.itainnova.es/maturolife/sw.js", mode='rt', 
                 cache_storage='/tmp/cache1',
                 target_protocol='s3', target_options={'anon': True})
with of as f:
    print(f.readline())
add_library(object_archive
  object_archive.cpp
)

target_link_libraries(object_archive
  ${Boost_LIBRARIES}
)

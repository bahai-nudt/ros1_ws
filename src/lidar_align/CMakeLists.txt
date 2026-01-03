cmake_minimum_required(VERSION 2.8.3)
project(lidar_align)

add_definitions(-std=c++11 -ofast)

set(CMAKE_CXX_STANDARD 14)

find_package(catkin REQUIRED COMPONENTS
  roscpp
  image_transport
  cv_bridge
  pcl_ros
  rosbag
  sensor_msgs
  geometry_msgs
  message_generation
)

# NLOPT is frustratingly inconsistent in the name of its cmake file so we use our own
list(APPEND CMAKE_FIND_ROOT_PATH ${CMAKE_SOURCE_DIR})
list(APPEND CMAKE_FIND_ROOT_PATH ${PROJECT_SOURCE_DIR})
set (CMAKE_PREFIX_PATH "/usr/local/lib/cmake/nlopt")
find_package(NLOPT REQUIRED)


include_directories(
  include
  msg
  ${NLOPT_INCLUDE_DIRS}
  ${catkin_INCLUDE_DIRS}
)

add_message_files(
  FILES
  Oem7RawMsg.msg
  Oem7Header.msg
  BESTPOS.msg
  BESTUTM.msg
  BESTVEL.msg
  BESTGNSSPOS.msg
  PPPPOS.msg
  HEADING2.msg
  INSCONFIG.msg
  INSPVA.msg
  INSPVAX.msg
  RXSTATUS.msg
  INSSTDEV.msg
  CORRIMU.msg
  CORRIMUDATA.msg
  IMURATECORRIMU.msg
  TERRASTARSTATUS.msg
  TERRASTARINFO.msg
  TIME.msg
  INSExtendedSolutionStatus.msg
  INSFrame.msg
  INSReceiverStatus.msg
  INSResetType.msg
  INSSourceStatus.msg
  INSUpdate.msg
  BestExtendedSolutionStatus.msg
  InertialSolutionStatus.msg
  INSAlignmentMode.msg
  INSOutputFrame.msg
  PositionOrVelocityType.msg
  Rotation.msg
  RotationalOffset.msg
  SolutionStatus.msg
  SolutionSource.msg
  Translation.msg
  TranslationOffset.msg
  SubscriptionPermission.msg
  SubscriptionType.msg
  SyncState.msg
  RegionRestriction.msg
  LocalAreaStatus.msg
  AccessStatus.msg
  GeogatingStatus.msg
  BYINS2.msg
  GNSSSolutionStatus.msg
  INSPVAXExtendedParams.msg
  Localization.msg
  LocalMap.msg
  Point64.msg
  Points64.msg
  TaskInfo.msg
  ParkSpot.msg

)

generate_messages(
  DEPENDENCIES
  std_msgs
  sensor_msgs
  geometry_msgs
)

add_library(${PROJECT_NAME}
  src/sensors.cpp
  src/aligner.cpp  
  src/loader.cpp
)
target_link_libraries(${PROJECT_NAME} ${catkin_LIBRARIES} ${NLOPT_LIBRARIES})

catkin_package(
   INCLUDE_DIRS include/
   LIBRARIES ${PROJECT_NAME}

  CATKIN_DEPENDS
  roscpp image_transport cv_bridge pcl_ros rosbag sensor_msgs geometry_msgs
  message_runtime
)

add_executable(lidar_align_node
  src/lidar_align_node.cpp
  
)
target_link_libraries(lidar_align_node ${PROJECT_NAME} ${catkin_LIBRARIES})

install(DIRECTORY include/lidar_align
  DESTINATION include
  FILES_MATCHING 
    PATTERN "*.h"
    PATTERN "*.hpp"
)

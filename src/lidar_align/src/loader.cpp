#include <geometry_msgs/TransformStamped.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>
#include <iomanip>

#include "lidar_align/loader.h"
#include "lidar_align/transform.h"

namespace lidar_align {

Loader::Loader(const Config& config) : config_(config) {}

Loader::Config Loader::getConfig(ros::NodeHandle* nh) {
  Loader::Config config;
  nh->param("use_n_scans", config.use_n_scans, config.use_n_scans);
  return config;
}

void Loader::parsePointcloudMsg(const sensor_msgs::PointCloud2 msg,
                                LoaderPointcloud* pointcloud) {
  bool has_timing = false;
  bool has_intensity = false;
  for (const sensor_msgs::PointField& field : msg.fields) {
    if (field.name == "time_offset_us") {
      has_timing = true;
    } else if (field.name == "intensity") {
      has_intensity = true;
    }
  }

  if (has_timing) {
    pcl::fromROSMsg(msg, *pointcloud);
    return;
  } else if (has_intensity) {



    LoaderPointcloud16 raw_pointcloud;
    pcl::fromROSMsg(msg, raw_pointcloud);


    Timestamp stamp = msg.header.stamp.sec * 1000000ll +
                      msg.header.stamp.nsec / 1000ll;

    for (const PointXYZIT_uint16& raw_point : raw_pointcloud) {

      PointAllFields point;
      point.x = raw_point.x;
      point.y = raw_point.y;
      point.z = raw_point.z;
      point.intensity = raw_point.intensity;
      point.time_offset_us = static_cast<int64_t>(raw_point.timestamp * 1000000ll ) - static_cast<int64_t>(stamp);


      // std::cout << std::setprecision(16) << stamp << "," << raw_point.timestamp << std::endl;



      // std::cout << "raw_point.timestamp: " << raw_point.timestamp << std::endl;




    // Pointcloud raw_pointcloud;
    // pcl::fromROSMsg(msg, raw_pointcloud);

    // for (const Point& raw_point : raw_pointcloud) {
    //   PointAllFields point;
    //   point.x = raw_point.x;
    //   point.y = raw_point.y;
    //   point.z = raw_point.z;
    //   point.intensity = raw_point.intensity;

      if (!std::isfinite(point.x) || !std::isfinite(point.y) ||
          !std::isfinite(point.z) || !std::isfinite(point.intensity)) {
        continue;
      }

      if (std::sqrt(point.x * point.x + point.y * point.y) < 15) {
        continue;
      }

      pointcloud->push_back(point);
    }

    // std::exit(-1);

    // std::exit(-1);
    pointcloud->header = raw_pointcloud.header;
  } else {
    pcl::PointCloud<pcl::PointXYZ> raw_pointcloud;
    pcl::fromROSMsg(msg, raw_pointcloud);

    for (const pcl::PointXYZ& raw_point : raw_pointcloud) {
      PointAllFields point;
      point.x = raw_point.x;
      point.y = raw_point.y;
      point.z = raw_point.z;

      if (!std::isfinite(point.x) || !std::isfinite(point.y) ||
          !std::isfinite(point.z)) {
        continue;
      }

      pointcloud->push_back(point);
    }
    pointcloud->header = raw_pointcloud.header;
  }
}

bool Loader::loadPointcloudFromROSBag(const std::string& bag_path,
                                      const Scan::Config& scan_config,
                                      Lidar* lidar) {
  rosbag::Bag bag;
  try {
    bag.open(bag_path, rosbag::bagmode::Read);
  } catch (rosbag::BagException e) {
    ROS_ERROR_STREAM("LOADING BAG FAILED: " << e.what());
    return false;
  }

  std::vector<std::string> types;
  types.push_back(std::string("sensor_msgs/PointCloud2"));
  rosbag::View view(bag, rosbag::TypeQuery(types));

  size_t scan_num = 0;
  for (const rosbag::MessageInstance& m : view) {
    std::cout << " Loading scan: \e[1m" << scan_num++ << "\e[0m from ros bag"
              << '\r' << std::flush;
  //   if (scan_num < 0 || scan_num > 265) {
	// continue;
  //   }
    LoaderPointcloud pointcloud;
    parsePointcloudMsg(*(m.instantiate<sensor_msgs::PointCloud2>()),
                       &pointcloud);

    lidar->addPointcloud(pointcloud, scan_config);

    if (lidar->getNumberOfScans() >= config_.use_n_scans) {
      break;
    }
  }
  if (lidar->getTotalPoints() == 0) {
    ROS_ERROR_STREAM(
        "No points were loaded, verify that the bag contains populated "
        "messages of type sensor_msgs/PointCloud2");
    return false;
  }

  return true;
}

bool Loader::loadTformFromROSBag(const std::string& bag_path, Odom* odom) {
  rosbag::Bag bag;
  try {
    bag.open(bag_path, rosbag::bagmode::Read);
  } catch (rosbag::BagException e) {
    ROS_ERROR_STREAM("LOADING BAG FAILED: " << e.what());
    return false;
  }

  std::vector<std::string> types;
  types.push_back(std::string("geometry_msgs/TransformStamped"));
  rosbag::View view(bag, rosbag::TypeQuery(types));

  size_t tform_num = 0;
  for (const rosbag::MessageInstance& m : view) {
    std::cout << " Loading transform: \e[1m" << tform_num++
              << "\e[0m from ros bag" << '\r' << std::flush;

    geometry_msgs::TransformStamped transform_msg =
        *(m.instantiate<geometry_msgs::TransformStamped>());

    Timestamp stamp = transform_msg.header.stamp.sec * 1000000ll +
                      transform_msg.header.stamp.nsec / 1000ll;

    Transform T(Transform::Translation(transform_msg.transform.translation.x,
                                       transform_msg.transform.translation.y,
                                       transform_msg.transform.translation.z),
                Transform::Rotation(transform_msg.transform.rotation.w,
                                    transform_msg.transform.rotation.x,
                                    transform_msg.transform.rotation.y,
                                    transform_msg.transform.rotation.z));
    odom->addTransformData(stamp, T);
  }

  if (odom->empty()) {
    ROS_ERROR_STREAM("No odom messages found!");
    return false;
  }

  return true;
}


bool Loader::loadTformFromROSBag_novaltel(const std::string& bag_path, Odom* odom) {
  rosbag::Bag bag;
  try {
    bag.open(bag_path, rosbag::bagmode::Read);
  } catch (rosbag::BagException e) {
    ROS_ERROR_STREAM("LOADING BAG FAILED: " << e.what());
    return false;
  }

  std::vector<std::string> types;
  types.push_back(std::string("novatel_oem7_msgs/INSPVAX"));
  rosbag::View view(bag, rosbag::TypeQuery(types));

  size_t count = 0;
  for (const rosbag::MessageInstance& m : view) {
    std::cout << " Loading INSPVAX: \e[1m" << count++
              << "\e[0m from ros bag" << '\r' << std::flush;

    auto inspva_msg = m.instantiate<lidar_align::INSPVAX>();

    if(!inspva_msg)
      continue;

    Timestamp stamp = inspva_msg->header.stamp.sec * 1000000ll +
                      inspva_msg->header.stamp.nsec / 1000ll;

    double x, y;
    int zone;
    double longitude = inspva_msg->longitude;
    double latitude = inspva_msg->latitude;

    lla2utm(longitude, latitude, x, y, zone); 

    double yaw = 90.0 - inspva_msg->azimuth;

    while (yaw < 0.0) yaw += 360.0;
    while (yaw >= 360.0) yaw -= 360.0;

    Eigen::AngleAxisd rollAngle(inspva_msg->roll / 180 * M_PI, Eigen::Vector3d::UnitY());
    Eigen::AngleAxisd pitchAngle(inspva_msg->pitch / 180 * M_PI, Eigen::Vector3d::UnitX());
    Eigen::AngleAxisd yawAngle(yaw / 180 * M_PI, Eigen::Vector3d::UnitZ());

    Eigen::Quaterniond q = yawAngle * pitchAngle * rollAngle;


    // Eigen::Quaterniond q = toQuaternionZYX(
    //         inspva_msg->roll / 180 * M_PI, 
    //         inspva_msg->pitch / 180 * M_PI,
    //         inspva_msg->azimuth /180 * M_PI);


    Transform T(Transform::Translation(x - 602177, y - 4555491, inspva_msg->height - 680),
                Transform::Rotation(q.w(), q.x(), q.y(), q.z()));
    

    odom->addTransformData(stamp, T);
  }

  if (odom->empty()) {
    ROS_ERROR_STREAM("No odom messages found!");
    return false;
  }

  return true;
}


bool Loader::loadTformFromMaplabCSV(const std::string& csv_path, Odom* odom) {
  std::ifstream file(csv_path, std::ifstream::in);

  size_t tform_num = 0;
  while (file.peek() != EOF) {
    std::cout << " Loading transform: \e[1m" << tform_num++
              << "\e[0m from csv file" << '\r' << std::flush;

    Timestamp stamp;
    Transform T;

    if (getNextCSVTransform(file, &stamp, &T)) {
      odom->addTransformData(stamp, T);
    }
  }

  return true;
}

// lots of potential failure cases not checked
bool Loader::getNextCSVTransform(std::istream& str, Timestamp* stamp,
                                 Transform* T) {
  std::string line;
  std::getline(str, line);

  // ignore comment lines
  if (line[0] == '#') {
    return false;
  }

  std::stringstream line_stream(line);
  std::string cell;

  std::vector<std::string> data;
  while (std::getline(line_stream, cell, ',')) {
    data.push_back(cell);
  }

  if (data.size() < 9) {
    return false;
  }

  constexpr size_t TIME = 0;
  constexpr size_t X = 2;
  constexpr size_t Y = 3;
  constexpr size_t Z = 4;
  constexpr size_t RW = 5;
  constexpr size_t RX = 6;
  constexpr size_t RY = 7;
  constexpr size_t RZ = 8;

  *stamp = std::stoll(data[TIME]) / 1000ll;
  *T = Transform(Transform::Translation(std::stod(data[X]), std::stod(data[Y]),
                                        std::stod(data[Z])),
                 Transform::Rotation(std::stod(data[RW]), std::stod(data[RX]),
                                     std::stod(data[RY]), std::stod(data[RZ])));

  return true;
}


Eigen::Quaterniond Loader::toQuaternionZYX(double roll, double pitch, double yaw) {

    yaw = M_PI / 2.0 - yaw;

    // 规范化到 [0, 360) 范围
    //while (yaw < 0.0) yaw += M_PI * 2;
    //while (yaw >= M_PI * 2) yaw -= M_PI * 2;


    Eigen::AngleAxisd rollAngle(roll, Eigen::Vector3d::UnitX());
    Eigen::AngleAxisd pitchAngle(pitch, Eigen::Vector3d::UnitY());
    Eigen::AngleAxisd yawAngle(yaw , Eigen::Vector3d::UnitZ());

    Eigen::Quaterniond q = yawAngle * pitchAngle * rollAngle;
    q.normalize();
    return q;
}

void Loader::lla2utm(double lon_deg, double lat_deg, double& x, double& y, int& zone) {

// WGS84 椭球体参数
    double EQUATORIAL_RADIUS = 6378137.0;        // 赤道半径 (米)
    double POLAR_RADIUS = 6356752.314245;        // 极半径 (米)
    double FLATTENING = 1.0 / 298.257223563;     // 扁率
    double ECCENTRICITY_SQ = 2 * FLATTENING - FLATTENING * FLATTENING; // 第一偏心率平方
    
    // UTM 参数
    double SCALE_FACTOR = 0.9996;                // 比例因子
    double FALSE_EASTING = 500000.0;             // 假东位移 (米)
    double FALSE_NORTHING_N = 0.0;               // 北半球假北位移
    double FALSE_NORTHING_S = 10000000.0;        // 南半球假北位移

    // 将度转换为弧度
    double lat_rad = lat_deg * M_PI / 180.0;
    double lon_rad = lon_deg * M_PI / 180.0;
    
    // 计算 UTM 带号和纬度带
    zone = static_cast<int>((lon_deg + 180.0) / 6.0) + 1;
        
    // 计算中央子午线经度
    double central_meridian = (zone - 1) * 6.0 - 180.0 + 3.0; // 中央子午线经度
    double central_meridian_rad = central_meridian * M_PI / 180.0;
    
    // 计算经度差
    double delta_lon = lon_rad - central_meridian_rad;
    
    // 计算椭球参数
    double e2 = ECCENTRICITY_SQ;
    double e4 = e2 * e2;
    double e6 = e4 * e2;
    double e8 = e4 * e4;
    
    double n = (EQUATORIAL_RADIUS - POLAR_RADIUS) / (EQUATORIAL_RADIUS + POLAR_RADIUS);
    double n2 = n * n;
    double n3 = n2 * n;
    double n4 = n3 * n;
    
    // 计算子午线弧长
    double A = EQUATORIAL_RADIUS * (1.0 - n + 1.25 * (n2 - n3) + (81.0/64.0) * (n4 - n3));
    double B = (3.0 * EQUATORIAL_RADIUS * n / 2.0) * (1.0 - n + (7.0/8.0) * (n2 - n3));
    double C = (15.0 * EQUATORIAL_RADIUS * n2 / 16.0) * (1.0 - n + (3.0/4.0) * n2);
    double D = (35.0 * EQUATORIAL_RADIUS * n3 / 48.0) * (1.0 - n);
    double E = (315.0 * EQUATORIAL_RADIUS * n4 / 512.0) * (1.0);
    
    double meridian = A * lat_rad - B * sin(2 * lat_rad) + C * sin(4 * lat_rad) 
                        - D * sin(6 * lat_rad) + E * sin(8 * lat_rad);
    
    // 计算卯酉圈曲率半径
    double v = EQUATORIAL_RADIUS / sqrt(1 - e2 * sin(lat_rad) * sin(lat_rad));
    double p = v * (1 - e2) / (1 - e2 * sin(lat_rad) * sin(lat_rad));
    
    // 计算参数
    double cos_lat = cos(lat_rad);
    double sin_lat = sin(lat_rad);
    double tan_lat = tan(lat_rad);
        
    double T = tan_lat * tan_lat;
    double C_ = e2 * cos_lat * cos_lat / (1 - e2);
    double A_ = delta_lon * cos_lat;
    double A2 = A_ * A_;
    double A3 = A2 * A_;
    double A4 = A3 * A_;
    double A5 = A4 * A_;
    double A6 = A5 * A_;
    
    // 计算东坐标和北坐标
    double M = meridian;
    x = SCALE_FACTOR * v * (A_ + (1 - T + C_) * A3 / 6.0 
    + (5 - 18 * T + T * T + 72 * C_ - 58 * e2) * A5 / 120.0) 
    + FALSE_EASTING;
    
    y = SCALE_FACTOR * (M + v * tan_lat * (A2 / 2.0 
    + (5 - T + 9 * C_ + 4 * C_ * C_) * A4 / 24.0 
    + (61 - 58 * T + T * T + 600 * C_ - 330 * e2) * A6 / 720.0));
    
    // 南半球调整
    if (lat_deg < 0) {
        y += FALSE_NORTHING_S;
    }
}

}  // namespace lidar_align

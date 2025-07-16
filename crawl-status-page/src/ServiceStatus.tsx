/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import { useEffect, useState } from "react";

const ServiceStatusList = () => {
  const [graphReachable, setGraphReachable] = useState(false);
  const [s3Reachable, setS3Reachable] = useState(false);
  const [docsReachable, setDocsReachable] = useState(false);

  useEffect(() => {
    const checkDocsReachability = async () => {
      try {
        const response = await fetch("https://docs.geoconnex.us");
        setDocsReachable(response.ok);
      } catch (error) {
        console.error("Error checking docs reachability:", error);
        setDocsReachable(false);
      }
    };

    void checkDocsReachability();
  }, []);

  useEffect(() => {
    const checkS3Reachability = async () => {
      try {
        const response = await fetch("https://geoconnex.us");
        setS3Reachable(response.ok);
      } catch (error) {
        console.error("Error checking S3 reachability:", error);
        setS3Reachable(false);
      }
    };

    void checkS3Reachability();
  }, []);

  useEffect(() => {
    const checkGraphReachability = async () => {
      try {
        const response = await fetch("https://graph.geoconnex.us");
        setGraphReachable(response.ok);
      } catch (error) {
        console.error("Error checking graph reachability:", error);
        setGraphReachable(false);
      }
    };

    void checkGraphReachability();
  }, []);

  return (
    <div>
      <h2>Reachable Services:</h2>
      <p>Graph: {graphReachable ? "Yes" : "No"}, S3: {s3Reachable ? "Yes" : "No"}, Docs: {docsReachable ? "Yes" : "No"}</p>
    </div>
  );
};

export default ServiceStatusList;

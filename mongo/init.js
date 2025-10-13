// Function to check if the node is a primary
try {
  // eslint-disable-next-line no-undef
  rs.status();
} catch (e) {
  if (e.code === 94) {
    console.log("Initializing replica set...");
    // eslint-disable-next-line no-undef
    rs.initiate();
    // eslint-disable-next-line no-undef
    sleep(300);
  } else {
    console.error("Error checking replica set status:", e);
    throw e;
  }
}

console.log("Replica set initialized.");

// Check if root user exists
// eslint-disable-next-line no-undef
const adminDb = db.getSiblingDB("admin");
const rootUser = adminDb.getUser("root");

if (!rootUser) {
  console.log("Creating root user...");
  try {
    adminDb.createUser({
      user: "root",
      pwd: "password",
      roles: [{ role: "root", db: "admin" }],
    });
  } catch (e) {
    console.error("Error creating root user:", e);
    throw e;
  }
}

console.log("Root user exists or created successfully.");

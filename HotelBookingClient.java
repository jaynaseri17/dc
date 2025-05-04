import java.rmi.Naming;
import java.util.Scanner;

public class HotelBookingClient {
    public static void main(String[] args) {
        try {
            // Look up the remote object in the RMI registry
            HotelBooking hotel = (HotelBooking) Naming.lookup("rmi://localhost/HotelBookingService");
            
            Scanner scanner = new Scanner(System.in);
            
            while (true) {
                System.out.println("\nHotel Booking System");
                System.out.println("1. Book a room");
                System.out.println("2. Cancel booking");
                System.out.println("3. View current bookings");
                System.out.println("4. Exit");
                System.out.print("Enter your choice: ");
                
                int choice = scanner.nextInt();
                scanner.nextLine(); // consume newline
                
                switch (choice) {
                    case 1:
                        System.out.print("Enter guest name: ");
                        String guestName = scanner.nextLine();
                        System.out.print("Enter room number (1-10): ");
                        int roomNumber = scanner.nextInt();
                        System.out.println(hotel.bookRoom(guestName, roomNumber));
                        break;
                    case 2:
                        System.out.print("Enter guest name to cancel booking: ");
                        String cancelName = scanner.nextLine();
                        System.out.println(hotel.cancelBooking(cancelName));
                        break;
                    case 3:
                        System.out.println(hotel.listBookings());
                        break;
                    case 4:
                        System.out.println("Exiting...");
                        scanner.close();
                        System.exit(0);
                    default:
                        System.out.println("Invalid choice");
                }
            }
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }
}